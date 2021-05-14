package rpc

import (
	"context"
	"net"
	"net/http"

	"github.com/Conflux-Chain/go-conflux-sdk/rpc"
	infuraNode "github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/ethereum/go-ethereum/node"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	namespaceCfx     = "cfx"
	namespaceTrace   = "trace"
	namespaceMetrics = "metrics"
)

var (
	stdServer *http.Server
)

// Serve starts to serve RPC requests.
func Serve(endpoint string, router infuraNode.Router, db store.Store) {
	clientProvider := infuraNode.NewClientProvider(router)
	handler := rpc.NewServer()

	mustRegisterService(handler, namespaceCfx, newCfxAPI(clientProvider, db))
	mustRegisterService(handler, namespaceTrace, newTraceAPI(clientProvider))
	mustRegisterService(handler, namespaceMetrics, &metricsAPI{})

	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to listen to endpoint %v", endpoint)
	}

	server := http.Server{
		Handler: node.NewHTTPHandlerStack(handler, []string{"*"}, []string{"*"}),
	}
	stdServer = &server

	logrus.Info("JSON RPC services started...")

	server.Serve(listener)
}

func Shutdown(ctx context.Context) error {
	if stdServer != nil {
		return stdServer.Shutdown(ctx)
	}
	return nil
}

func mustRegisterService(server *rpc.Server, namespace string, impl interface{}) {
	if err := server.RegisterName(namespace, impl); err != nil {
		panic(errors.WithMessagef(err, "Failed to register service [%v]", namespace))
	}
}
