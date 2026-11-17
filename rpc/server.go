package rpc

import (
	infuraNode "github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util/rate"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
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
	pubSubLimiter *rpcutil.PubSubLimiter,
	option ...CfxAPIOption,
) *rpcutil.Server {
	s, err := NewNativeSpaceServer(registry, clientProvider, gashandler, exposedModules, pubSubLimiter, option...)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize RPC server")
	}
	return s
}

func NewNativeSpaceServer(
	registry *rate.Registry,
	clientProvider *infuraNode.CfxClientProvider,
	gashandler *handler.CfxGasStationHandler,
	exposedModules []string,
	pubSubLimiter *rpcutil.PubSubLimiter,
	option ...CfxAPIOption,
) (*rpcutil.Server, error) {
	// retrieve all available core space rpc apis
	allApis := nativeSpaceApis(clientProvider, gashandler, registry, option...)

	exposedApis, err := filterExposedApis(allApis, exposedModules)
	if err != nil {
		return nil, err
	}

	middleware := httpMiddleware("cfx", registry, clientProvider)

	return rpcutil.NewServer(nativeSpaceRpcServerName, exposedApis, pubSubLimiter, middleware)
}

// MustNewEvmSpaceServer new evm space RPC server by specifying router, and exposed modules.
// `exposedModules` is a list of API modules to expose via the RPC interface. If the module
// list is empty, all RPC API endpoints designated public will be exposed.
func MustNewEvmSpaceServer(
	registry *rate.Registry,
	clientProvider *infuraNode.EthClientProvider,
	gasHandler *handler.EthGasStationHandler,
	exposedModules []string,
	pubSubLimiter *rpcutil.PubSubLimiter,
	option ...EthAPIOption,
) *rpcutil.Server {
	s, err := NewEvmSpaceServer(registry, clientProvider, gasHandler, exposedModules, pubSubLimiter, option...)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize RPC server")
	}
	return s
}

func NewEvmSpaceServer(
	registry *rate.Registry,
	clientProvider *infuraNode.EthClientProvider,
	gasHandler *handler.EthGasStationHandler,
	exposedModules []string,
	pubSubLimiter *rpcutil.PubSubLimiter,
	option ...EthAPIOption,
) (*rpcutil.Server, error) {
	// retrieve all available evm space rpc apis
	allApis, err := evmSpaceApis(clientProvider, gasHandler, registry, option...)
	if err != nil {
		return nil, err
	}

	exposedApis, err := filterExposedApis(allApis, exposedModules)
	if err != nil {
		return nil, err
	}

	middleware := httpMiddleware("eth", registry, clientProvider)

	return rpcutil.NewServer(evmSpaceRpcServerName, exposedApis, pubSubLimiter, middleware)
}

type CfxBridgeServerConfig struct {
	EthNode        string
	CfxNode        string
	ExposedModules []string
	Endpoint       string `default:":32537"`
}

func MustNewNativeSpaceBridgeServer(registry *rate.Registry, config *CfxBridgeServerConfig) *rpcutil.Server {
	s, err := NewNativeSpaceBridgeServer(registry, config)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize RPC server")
	}
	return s
}

func NewNativeSpaceBridgeServer(registry *rate.Registry, config *CfxBridgeServerConfig) (*rpcutil.Server, error) {
	allApis, cleanup, err := nativeSpaceBridgeApis(config)
	if err != nil {
		return nil, err
	}

	exposedApis, err := filterExposedApis(allApis, config.ExposedModules)
	if err != nil {
		cleanup()
		return nil, err
	}

	middleware := httpMiddleware("cfxBridge", registry, nil)
	srv, err := rpcutil.NewServer(nativeSpaceBridgeRpcServerName, exposedApis, nil, middleware)
	if err != nil {
		cleanup()
		return nil, err
	}
	srv.OnClose(cleanup)
	return srv, nil
}

// MustNewDebugServer new debug RPC server for internal debugging use.
func MustNewDebugServer() *rpcutil.Server {
	s, err := NewDebugServer()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize debug RPC server")
	}
	return s
}

func NewDebugServer() (*rpcutil.Server, error) {
	servedApis := make(map[string]interface{})
	for _, api := range debugApis() {
		servedApis[api.Namespace] = api.Service
	}

	return rpcutil.NewServer(debugRpcServerName, servedApis, nil)
}
