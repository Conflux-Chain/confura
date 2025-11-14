package rpc

import (
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc/cfxbridge"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util/metrics/service"
	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/pkg/errors"
)

// API describes the set of methods offered over the RPC interface
type API struct {
	Namespace string      // namespace under which the rpc methods of Service are exposed
	Version   string      // api version
	Service   interface{} // receiver instance which holds the methods
	Public    bool        // indication if the methods must be considered safe for public use
}

// Filter API modules by exposed modules settings.
// `exposedModules` is a setting list of API modules to expose via the RPC interface.
// If the module list is empty, all RPC API endpoints designated public will be exposed.
func filterExposedApis(allApis []API, exposedModules []string) (map[string]interface{}, error) {
	servedApis := make(map[string]interface{}, len(allApis))

	for _, api := range allApis {
		if len(exposedModules) == 0 { // empty module list, use all public RPC APIs
			if api.Public {
				servedApis[api.Namespace] = api.Service
			}
			continue
		}

		servedApis[api.Namespace] = api.Service
	}

	if len(exposedModules) == 0 {
		return servedApis, nil
	}

	filteredApis := make(map[string]interface{}, len(exposedModules))
	for _, m := range exposedModules {
		if svc, ok := servedApis[m]; ok {
			filteredApis[m] = svc
			continue
		}

		err := errors.Errorf("unknown module %v to be exposed", m)
		return map[string]interface{}{}, err
	}

	return filteredApis, nil
}

// nativeSpaceApis returns the collection of built-in RPC APIs for core space.
func nativeSpaceApis(
	clientProvider *node.CfxClientProvider,
	gashandler *handler.CfxGasStationHandler,
	registry *rate.Registry,
	option ...CfxAPIOption,
) []API {
	stateHandler := handler.NewCfxStateHandler(clientProvider)
	return []API{
		{
			Namespace: "cfx",
			Version:   "1.0",
			Service:   newCfxAPI(clientProvider, option...),
			Public:    true,
		}, {
			Namespace: "txpool",
			Version:   "1.0",
			Service:   &txPoolAPI{},
			Public:    true,
		}, {
			Namespace: "pos",
			Version:   "1.0",
			Service:   &posAPI{stateHandler},
			Public:    true,
		}, {
			Namespace: "trace",
			Version:   "1.0",
			Service:   &traceAPI{stateHandler},
			Public:    false,
		}, {
			Namespace: service.Namespace,
			Version:   "1.0",
			Service:   &service.MetricsAPI{},
			Public:    false,
		}, {
			Namespace: "gasstation",
			Version:   "1.0",
			Service:   newCfxGasStationAPI(gashandler),
			Public:    false,
		}, {
			Namespace: "debug",
			Version:   "1.0",
			Service:   &cfxDebugAPI{stateHandler},
			Public:    false,
		}, {
			Namespace: "diagnostic",
			Version:   "1.0",
			Service:   &diagnosticAPI{registry},
			Public:    false,
		},
	}
}

// evmSpaceApis returns the collection of built-in RPC APIs for EVM space.
func evmSpaceApis(
	clientProvider *node.EthClientProvider,
	gashandler *handler.EthGasStationHandler,
	registry *rate.Registry,
	option ...EthAPIOption) ([]API, error) {
	stateHandler := handler.NewEthStateHandler(clientProvider)
	return []API{
		{
			Namespace: "eth",
			Version:   "1.0",
			Service:   mustNewEthAPI(clientProvider, option...),
			Public:    true,
		}, {
			Namespace: "web3",
			Version:   "1.0",
			Service:   &web3API{},
			Public:    true,
		}, {
			Namespace: "net",
			Version:   "1.0",
			Service:   &netAPI{},
			Public:    true,
		}, {
			Namespace: "trace",
			Version:   "1.0",
			Service:   &ethTraceAPI{stateHandler},
			Public:    false,
		}, {
			Namespace: "parity",
			Version:   "1.0",
			Service:   &parityAPI{},
			Public:    false,
		}, {
			Namespace: "debug",
			Version:   "1.0",
			Service:   &ethDebugAPI{stateHandler},
			Public:    false,
		}, {
			Namespace: "txpool",
			Version:   "1.0",
			Service:   &ethTxPoolAPI{},
			Public:    false,
		}, {
			Namespace: "gasstation",
			Version:   "1.0",
			Service:   newEthGasStationAPI(gashandler),
		}, {
			Namespace: "diagnostic",
			Version:   "1.0",
			Service:   &diagnosticAPI{registry},
			Public:    false,
		},
	}, nil
}

// nativeSpaceBridgeApis adapts evm space RPCs to core space RPCs.
func nativeSpaceBridgeApis(config *CfxBridgeServerConfig) ([]API, error) {
	ethNodeURL, cfxNodeURL := config.EthNode, config.CfxNode
	eth, err := rpc.NewEthClient(ethNodeURL)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to connect to eth space")
	}

	ethChainId, err := eth.Eth.ChainId()
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to get chain ID from eth space")
	}
	rpc.HookMiddlewares(eth.Provider(), ethNodeURL, "eth")

	var cfx *sdk.Client
	if len(cfxNodeURL) > 0 { // optioinal
		cfx, err = rpc.NewCfxClient(cfxNodeURL)
		if err != nil {
			return nil, errors.WithMessage(err, "Failed to connect to cfx space")
		}

		rpc.HookMiddlewares(cfx.Provider(), cfxNodeURL, "cfx")
	}

	// Hook an event handler to reidrect HTTP headers for the sdk client request
	// because we could use `Confura` RPC service as our client endpoint for `cfxBridge`.
	rpc.HookRedirectHttpHeader()

	return []API{
		{
			Namespace: "cfx",
			Version:   "1.0",
			Service:   cfxbridge.NewCfxAPI(eth, uint32(*ethChainId), cfx),
			Public:    true,
		}, {
			Namespace: "trace",
			Version:   "1.0",
			Service:   cfxbridge.NewTraceAPI(eth, uint32(*ethChainId)),
			Public:    true,
		}, {
			Namespace: "txpool",
			Version:   "1.0",
			Service:   cfxbridge.NewTxpoolAPI(eth),
			Public:    true,
		},
	}, nil
}

// debugApis returns the collection of non-standard RPC methods for run time diagnostics and debug.
func debugApis() []API {
	return []API{
		{
			Namespace: "debug",
			Version:   "1.0",
			Service:   &debugAPI{},
			Public:    false,
		},
	}
}
