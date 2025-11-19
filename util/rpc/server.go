package rpc

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
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
	name    string
	servers map[Protocol]*http.Server
}

// MustNewServer creates an instance of Server with specified RPC services.
func MustNewServer(name string, rpcs map[string]any, middlewares ...handlers.Middleware) *Server {
	handler := rpc.NewServer()
	servedApis := make([]string, 0, len(rpcs))

	for namespace, impl := range rpcs {
		if err := handler.RegisterName(namespace, impl); err != nil {
			logrus.WithError(err).WithField("namespace", namespace).Fatal("Failed to register rpc service")
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
	wsServer := http.Server{
		Handler: handler.WebsocketHandler([]string{"*"}, rpc.WebsocketOption{
			WsPingInterval: viper.GetDuration("rpc.wsPingInterval"),
		}),
	}

	for i := len(middlewares) - 1; i >= 0; i-- {
		httpServer.Handler = middlewares[i](httpServer.Handler)
		wsServer.Handler = middlewares[i](wsServer.Handler)
	}

	return &Server{
		name: name,
		servers: map[Protocol]*http.Server{
			ProtocolHttp: &httpServer,
			ProtocolWS:   &wsServer,
		},
	}
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

// MustServeGraceful serves RPC server in a goroutine until graceful shutdown.
func (s *Server) MustServeGraceful(
	ctx context.Context, wg *sync.WaitGroup, endpoint string, protocol Protocol,
) {
	wg.Add(1)
	defer wg.Done()

	go s.MustServe(endpoint, protocol)

	<-ctx.Done()

	s.shutdown(protocol)
}

func (s *Server) shutdown(protocol Protocol) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
	defer cancel()

	logger := logrus.WithFields(logrus.Fields{
		"name":     s.name,
		"protocol": protocol,
	})

	if err := s.servers[protocol].Shutdown(ctx); err != nil {
		logger.WithError(err).Error("Failed to shutdown RPC server")
	} else {
		logger.Info("Succeed to shutdown RPC server")
	}
}

func (s *Server) String() string { return s.name }
