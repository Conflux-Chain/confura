package rpc

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	viperutil "github.com/Conflux-Chain/confura/util/viper"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Protocol string

const (
	ProtocolHttp = "HTTP"
	ProtocolWS   = "WS"
)

var (
	// DefaultShutdownTimeout is default timeout to shutdown RPC server.
	DefaultShutdownTimeout = 3 * time.Second

	// defaultWsPingInterval the default websocket ping/pong heartbeating interval.
	defaultWsPingInterval = 10 * time.Second
)

// Server serves JSON RPC services.
type Server struct {
	name           string
	servers        map[Protocol]*http.Server
	provider       *rpc.Server
	mu             sync.Mutex
	closing        bool
	active         map[Protocol]bool
	requests       sync.WaitGroup
	requestCancels map[*http.Request]context.CancelFunc
	closeOnce      sync.Once
	cleanup        []func()
}

// MustNewServer creates an instance of Server with specified RPC services.
func NewServer(
	name string,
	rpcs map[string]interface{},
	pubSubLimiter *PubSubLimiter,
	middlewares ...handlers.Middleware,
) (*Server, error) {
	var resrcLimit struct {
		MaxBatchRpcItemCount     int
		MaxBatchRpcResponseBytes int   `default:"10485760"` // default 10MB
		MaxHttpRequestBodyBytes  int   `default:"10485760"` // default 10MB
		MaxWebSocketMessageBytes int64 `default:"10485760"` // default 10MB
	}
	if err := viperutil.UnmarshalKey("requestControl.resourceLimits", &resrcLimit); err != nil {
		return nil, err
	}

	handler := rpc.NewServer()
	handler.SetBatchLimits(resrcLimit.MaxBatchRpcItemCount, resrcLimit.MaxBatchRpcResponseBytes)
	handler.SetHTTPBodyLimit(resrcLimit.MaxHttpRequestBodyBytes)
	handler.SetWebsocketReadLimit(resrcLimit.MaxWebSocketMessageBytes)

	servedApis := make([]string, 0, len(rpcs))

	for namespace, impl := range rpcs {
		if err := handler.RegisterName(namespace, impl); err != nil {
			return nil, fmt.Errorf("register RPC namespace %s: %w", namespace, err)
		}
		servedApis = append(servedApis, namespace)
	}

	logrus.WithFields(logrus.Fields{
		"APIs": servedApis,
		"name": name,
	}).Info("RPC server APIs registered")

	httpServer := http.Server{
		Handler: newHTTPHandlerStack(handler, []string{"*"}, []string{"*"}),
	}

	viper.SetDefault("rpc.wsPingInterval", defaultWsPingInterval)
	wsHandler := handler.WebsocketHandler([]string{"*"}, rpc.WebsocketOption{
		WsPingInterval: viper.GetDuration("rpc.wsPingInterval"),
	})
	if pubSubLimiter != nil {
		wsHandler = pubSubConnectionLimitMiddleware(pubSubLimiter)(wsHandler)
	}
	wsServer := http.Server{
		Handler: wsHandler,
	}

	for _, v := range slices.Backward(middlewares) {
		httpServer.Handler = v(httpServer.Handler)
		wsServer.Handler = v(wsServer.Handler)
	}

	s := &Server{
		provider: handler, active: make(map[Protocol]bool), requestCancels: make(map[*http.Request]context.CancelFunc),
		name: name,
		servers: map[Protocol]*http.Server{
			ProtocolHttp: &httpServer,
			ProtocolWS:   &wsServer,
		},
	}
	for _, srv := range s.servers {
		srv.Handler = s.trackRequests(srv.Handler)
	}
	return s, nil
}

func MustNewServer(name string, rpcs map[string]interface{}, limiter *PubSubLimiter, middlewares ...handlers.Middleware) *Server {
	s, err := NewServer(name, rpcs, limiter, middlewares...)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize RPC server")
	}
	return s
}

// MustServe serves RPC server in blocking way or panics if failed.
func (s *Server) MustServe(endpoint string, protocol Protocol) {
	logger := logrus.WithFields(logrus.Fields{
		"name":     s.name,
		"endpoint": endpoint,
		"protocol": protocol,
	})

	server, ok := s.servers[protocol]
	if !ok {
		logger.Fatal("RPC protocol unsupported")
	}

	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		logger.WithError(err).Fatal("Failed to listen to endpoint")
	}

	logger.Info("JSON RPC server started")

	server.Serve(listener)
}

// Start binds before returning and registers the task before launching it.
func (s *Server) Start(ctx context.Context, wg *sync.WaitGroup, endpoint string, protocol Protocol) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	srv, ok := s.servers[protocol]
	if !ok {
		return fmt.Errorf("unsupported RPC protocol %q", protocol)
	}
	if s.closing || s.active[protocol] {
		return fmt.Errorf("RPC server already started or stopped")
	}
	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		return fmt.Errorf("listen %s %s: %w", s.name, protocol, err)
	}
	s.active[protocol] = true
	srv.Addr = listener.Addr().String()
	wg.Add(1)
	go func() {
		defer wg.Done()
		served := make(chan struct{})
		go func() {
			defer close(served)
			if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
				logrus.WithError(err).Error("RPC listener stopped")
			}
		}()
		select {
		case <-ctx.Done():
		case <-served:
		}
		s.Close()
		<-served
	}()
	return nil
}

// MustServeGraceful is retained for callers outside the service startup path.
func (s *Server) MustServeGraceful(ctx context.Context, wg *sync.WaitGroup, endpoint string, protocol Protocol) {
	if err := s.Start(ctx, wg, endpoint, protocol); err != nil {
		logrus.WithError(err).Fatal("Failed to start RPC server")
	}
	<-ctx.Done()
	s.Close()
}

func (s *Server) trackRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			cancel()
			http.Error(w, "server shutting down", http.StatusServiceUnavailable)
			return
		}
		s.requestCancels[r] = cancel
		s.requests.Add(1)
		s.mu.Unlock()
		defer func() { cancel(); s.mu.Lock(); delete(s.requestCancels, r); s.mu.Unlock(); s.requests.Done() }()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Close drains HTTP, then cancels remaining calls and stops the shared provider
// once. It joins handlers, including hijacked WebSocket connections, before the
// owner can release databases. The process lifecycle bounds an uncooperative call.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		active := make([]*http.Server, 0, len(s.active))
		for protocol := range s.active {
			active = append(active, s.servers[protocol])
		}
		s.mu.Unlock()
		var wg sync.WaitGroup
		for _, srv := range active {
			wg.Add(1)
			go func(srv *http.Server) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
				defer cancel()
				if err := srv.Shutdown(ctx); err != nil {
					_ = srv.Close()
				}
			}(srv)
		}
		wg.Wait()
		s.mu.Lock()
		for _, cancel := range s.requestCancels {
			cancel()
		}
		s.mu.Unlock()
		s.provider.Stop()
		s.requests.Wait()
		for i := len(s.cleanup) - 1; i >= 0; i-- {
			s.cleanup[i]()
		}
	})
}

func (s *Server) String() string { return s.name }

// OnClose transfers resources to the server before it is started.
func (s *Server) OnClose(cleanup ...func()) { s.cleanup = append(s.cleanup, cleanup...) }
