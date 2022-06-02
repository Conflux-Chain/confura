package rpc

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/conflux-chain/conflux-infura/util"
	"github.com/conflux-chain/conflux-infura/util/rate"
	"github.com/conflux-chain/conflux-infura/util/rpc/middlewares"
	"github.com/ethereum/go-ethereum/node"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

type Protocol string

const (
	ProtocolHttp = "HTTP"
	ProtocolWS   = "WS"
)

// DefaultShutdownTimeout is default timeout to shutdown RPC server.
var DefaultShutdownTimeout = 3 * time.Second

// go-rpc-provider only supports static middlewares for RPC server.
func init() {
	// middlewares executed in order

	// rate limit
	rpc.HookHandleBatch(middlewares.RateLimitBatch)
	rpc.HookHandleCallMsg(middlewares.RateLimit)

	// metrics
	rpc.HookHandleBatch(middlewares.MetricsBatch)
	rpc.HookHandleCallMsg(middlewares.Metrics)

	// log
	rpc.HookHandleBatch(middlewares.LogBatch)
	rpc.HookHandleCallMsg(middlewares.Log)
}

// Server serves JSON RPC services.
type Server struct {
	name    string
	servers map[Protocol]*http.Server
}

// MustNewServer creates an instance of Server with specified RPC services.
func MustNewServer(name string, rpcs map[string]interface{}) *Server {
	return MustNewServerWithRateLimit(name, rpcs, nil)
}

// MustNewServerWithRateLimit creates an instance of Server with specified RPC services.
func MustNewServerWithRateLimit(name string, rpcs map[string]interface{}, registry *rate.Registry) *Server {
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

	httpHandler := node.NewHTTPHandlerStack(handler, []string{"*"}, []string{"*"})
	wsHandler := handler.WebsocketHandler([]string{"*"})

	return &Server{
		name: name,
		servers: map[Protocol]*http.Server{
			ProtocolHttp: newHttpServer(httpHandler, registry),
			ProtocolWS:   newHttpServer(wsHandler, registry),
		},
	}
}

func newHttpServer(handler http.Handler, registry *rate.Registry) *http.Server {
	handler = rate.HttpHandler(registry, handler)
	handler = util.NewIpHttpHandler(handler)
	return &http.Server{
		Handler: handler,
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
func (s *Server) MustServeGraceful(ctx context.Context, wg *sync.WaitGroup, endpoint string, protocol Protocol) {
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
