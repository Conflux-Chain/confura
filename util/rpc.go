package util

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/Conflux-Chain/go-conflux-sdk/rpc"
	"github.com/ethereum/go-ethereum/node"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

// RpcServer serves JSON RPC services.
type RpcServer struct {
	name string

	http *http.Server
	ws   *http.Server

	stop chan struct{} // Channel to wait for termination notifications
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

		http: &http.Server{
			Handler: node.NewHTTPHandlerStack(handler, []string{"*"}, []string{"*"}),
		},
		ws: &http.Server{
			Handler: handler.WebsocketHandler([]string{"*"}),
		},

		stop: make(chan struct{}),
	}
}

// MustServe serves RPC server with blocking until closed or panics if failed to listen to the
// specified endpoints starting with http endpoint then followed websocket endpoint (optional).
func (s *RpcServer) MustServe(endpoints ...string) {
	servers := []*http.Server{s.http, s.ws}

	for i := 0; i < len(servers); i++ {
		if i >= len(endpoints) || len(endpoints[i]) == 0 {
			continue
		}

		listener, err := net.Listen("tcp", endpoints[i])
		if err != nil {
			logrus.WithError(err).WithField("endpoint", endpoints[i]).Fatal("Failed to listen to endpoint")
		}

		logrus.WithField("endpoint", endpoints[i]).Info("JSON RPC server started")

		go servers[i].Serve(listener)
	}

	<-s.stop
}

// Close immediately closes the server.
func (s *RpcServer) Close() error {
	close(s.stop) // unblock

	return multierr.Combine(s.http.Close(), s.ws.Close())
}

// Shutdown gracefully shutdown the server.
func (s *RpcServer) Shutdown(timeout time.Duration) error {
	close(s.stop) // unblock

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return multierr.Combine(s.http.Shutdown(ctx), s.ws.Shutdown(ctx))
}

func (s *RpcServer) String() string { return s.name }
