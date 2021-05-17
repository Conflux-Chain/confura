package util

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/Conflux-Chain/go-conflux-sdk/rpc"
	"github.com/ethereum/go-ethereum/node"
	"github.com/sirupsen/logrus"
)

// RpcServer serves JSON RPC services.
type RpcServer struct {
	name   string
	server *http.Server
}

// MustNewRpcServer creates an instance of RpcServer with specified RPC services.
func MustNewRpcServer(name string, rpcs map[string]interface{}) *RpcServer {
	handler := rpc.NewServer()

	for namespace, impl := range rpcs {
		if err := handler.RegisterName(namespace, impl); err != nil {
			logrus.WithError(err).WithField("namespace", namespace).Fatal("Failed to register rpc service")
		}
	}

	return &RpcServer{
		name: name,
		server: &http.Server{
			Handler: node.NewHTTPHandlerStack(handler, []string{"*"}, []string{"*"}),
		},
	}
}

// MustServe serves RPC in synchronized way or panics if failed to listen to the specified endpoint.
func (s *RpcServer) MustServe(endpoint string) {
	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		logrus.WithError(err).WithField("endpoint", endpoint).Fatal("Failed to listen to endpoint")
	}

	logrus.WithField("endpoint", endpoint).Info("JSON RPC server started")

	s.server.Serve(listener)
}

// Close immediately closes the server.
func (s *RpcServer) Close() error {
	return s.server.Close()
}

// Shutdown gracefully shutdown the server.
func (s *RpcServer) Shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return s.server.Shutdown(ctx)
}

func (s *RpcServer) String() string { return s.name }
