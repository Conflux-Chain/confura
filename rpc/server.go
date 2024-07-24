package rpc

import (
	infuraNode "github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/Conflux-Chain/confura/util/rpc"
	"github.com/sirupsen/logrus"
)

const (
	nativeSpaceRpcServerName = "core_space_rpc"
	evmSpaceRpcServerName    = "evm_space_rpc"

	nativeSpaceBridgeRpcServerName = "core_space_bridge_rpc"

	debugRpcServerName = "debug_rpc"
)

// MustNewNativeSpaceServer new core space RPC server by specifying router, handler
// and exposed modules.  Argument exposedModules is a list of API modules to expose
// via the RPC interface. If the module list is empty, all RPC API endpoints designated
// public will be exposed.
func MustNewNativeSpaceServer(
	registry *rate.Registry,
	clientProvider *infuraNode.CfxClientProvider,
	gashandler *handler.CfxGasStationHandler,
	exposedModules []string,
	option ...CfxAPIOption,
) *rpc.Server {
	// retrieve all available core space rpc apis
	allApis := nativeSpaceApis(clientProvider, gashandler, option...)

	exposedApis, err := filterExposedApis(allApis, exposedModules)
	if err != nil {
		logrus.WithError(err).Fatal(
			"Failed to new native space RPC server with bad exposed modules",
		)
	}

	middleware := httpMiddleware(registry, clientProvider)

	return rpc.MustNewServer(nativeSpaceRpcServerName, exposedApis, middleware)
}

// MustNewEvmSpaceServer new evm space RPC server by specifying router, and exposed modules.
// `exposedModules` is a list of API modules to expose via the RPC interface. If the module
// list is empty, all RPC API endpoints designated public will be exposed.
func MustNewEvmSpaceServer(
	registry *rate.Registry,
	clientProvider *infuraNode.EthClientProvider,
	gasHandler *handler.EthGasStationHandler,
	exposedModules []string,
	option ...EthAPIOption,
) *rpc.Server {
	// retrieve all available evm space rpc apis
	allApis, err := evmSpaceApis(clientProvider, gasHandler, option...)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new EVM space RPC server")
	}

	exposedApis, err := filterExposedApis(allApis, exposedModules)
	if err != nil {
		logrus.WithError(err).Fatal(
			"Failed to new EVM space RPC server with bad exposed modules",
		)
	}

	middleware := httpMiddleware(registry, clientProvider)

	return rpc.MustNewServer(evmSpaceRpcServerName, exposedApis, middleware)
}

type CfxBridgeServerConfig struct {
	EthNode              string
	CfxNode              string
	BatchRcptImpl        int
	BatchRcptConcurrency int `default:"4"`
	ExposedModules       []string
	Endpoint             string `default:":32537"`
}

func MustNewNativeSpaceBridgeServer(registry *rate.Registry, config *CfxBridgeServerConfig) *rpc.Server {
	allApis, err := nativeSpaceBridgeApis(config)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new CFX bridge RPC server")
	}

	exposedApis, err := filterExposedApis(allApis, config.ExposedModules)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new CFX bridge RPC server with bad exposed modules")
	}

	middleware := httpMiddleware(registry, nil)
	return rpc.MustNewServer(nativeSpaceBridgeRpcServerName, exposedApis, middleware)
}

// MustNewDebugServer new debug RPC server for internal debugging use.
func MustNewDebugServer() *rpc.Server {
	servedApis := make(map[string]interface{})
	for _, api := range debugApis() {
		servedApis[api.Namespace] = api.Service
	}

	return rpc.MustNewServer(debugRpcServerName, servedApis)
}
