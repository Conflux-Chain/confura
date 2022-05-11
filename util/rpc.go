package util

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/conflux-chain/conflux-infura/util/rate"
	"github.com/ethereum/go-ethereum/node"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

type RpcProtocol string

const (
	RpcProtocolHttp = "HTTP"
	RpcProtocolWS   = "WS"
)

// DefaultShutdownTimeout is default timeout to shutdown RPC server.
var DefaultShutdownTimeout = 3 * time.Second

// RpcServer serves JSON RPC services.
type RpcServer struct {
	name    string
	servers map[RpcProtocol]*http.Server
}

// MustNewRpcServer creates an instance of RpcServer with specified RPC services.
func MustNewRpcServer(name string, rpcs map[string]interface{}) *RpcServer {
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

	return &RpcServer{
		name: name,
		servers: map[RpcProtocol]*http.Server{
			RpcProtocolHttp: {
				Handler: rateLimitHandler(node.NewHTTPHandlerStack(handler, []string{"*"}, []string{"*"})),
			},
			RpcProtocolWS: {
				Handler: rateLimitHandler(handler.WebsocketHandler([]string{"*"})),
			},
		},
	}
}

func rateLimitHandler(next http.Handler) http.Handler {
	// TODO read from configuration file and support reload.
	limiter := rate.DefaultRegistry.GetOrRegister("httpRequest", 5, 100)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ip := GetIPAddress(r); limiter.Allow(ip, 1) {
			next.ServeHTTP(w, r)
		} else {
			http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
		}
	})
}

// MustServe serves RPC server in blocking way or panics if failed.
func (s *RpcServer) MustServe(endpoint string, protocol RpcProtocol) {
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
func (s *RpcServer) MustServeGraceful(ctx context.Context, wg *sync.WaitGroup, endpoint string, protocol RpcProtocol) {
	wg.Add(1)
	defer wg.Done()

	go s.MustServe(endpoint, protocol)

	<-ctx.Done()

	s.shutdown(protocol)
}

func (s *RpcServer) shutdown(protocol RpcProtocol) {
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

func (s *RpcServer) String() string { return s.name }
