package rpc

import (
	"net"
	"net/http"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/rpc"
	"github.com/ethereum/go-ethereum/node"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	namespaceCfx     = "cfx"
	namespaceTrace   = "trace"
	namespaceMetrics = "metrics"
)

// Serve starts to serve RPC requests.
func Serve(endpoint string, cfx sdk.ClientOperator) {
	handler := rpc.NewServer()

	mustRegisterService(handler, namespaceCfx, newCfxAPI(cfx))
	mustRegisterService(handler, namespaceTrace, newTraceAPI(cfx))
	mustRegisterService(handler, namespaceMetrics, &metricsAPI{})

	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to listen to endpoint %v", endpoint)
	}

	server := http.Server{
		Handler: node.NewHTTPHandlerStack(handler, []string{"*"}, []string{"*"}),
	}

	logrus.Info("JSON RPC services started...")

	server.Serve(listener)
}

func mustRegisterService(server *rpc.Server, namespace string, impl interface{}) {
	if err := server.RegisterName(namespace, impl); err != nil {
		panic(errors.WithMessagef(err, "Failed to register service [%v]", namespace))
	}
}
