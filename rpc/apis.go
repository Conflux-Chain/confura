package rpc

import (
	infuraNode "github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/rpc/cfxbridge"
	"github.com/conflux-chain/conflux-infura/rpc/handler"
	"github.com/pkg/errors"
)

// API describes the set of methods offered over the RPC interface
type API struct {
	Namespace string      // namespace under which the rpc methods of Service are exposed
	Version   string      // api version for DApp's
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

		err := errors.Errorf("unkown module %v to be exposed", m)
		return map[string]interface{}{}, err
	}

	return filteredApis, nil
}

// nativeSpaceApis returns the collection of built-in RPC APIs for native space.
func nativeSpaceApis(
	router infuraNode.Router, gashandler *handler.GasStationHandler, option ...CfxAPIOption,
) []API {
	clientProvider := infuraNode.NewCfxClientProvider(router)

	return []API{
		{
			Namespace: "cfx",
			Version:   "1.0",
			Service:   newCfxAPI(clientProvider, option...),
			Public:    true,
		}, {
			Namespace: "txpool",
			Version:   "1.0",
			Service:   &txPoolAPI{clientProvider},
			Public:    true,
		}, {
			Namespace: "pos",
			Version:   "1.0",
			Service:   newPosAPI(clientProvider),
			Public:    true,
		}, {
			Namespace: "trace",
			Version:   "1.0",
			Service:   newTraceAPI(clientProvider),
			Public:    false,
		}, {
			Namespace: "metrics",
			Version:   "1.0",
			Service:   &metricsAPI{},
			Public:    false,
		}, {
			Namespace: "gasstation",
			Version:   "1.0",
			Service:   newGasStationAPI(gashandler),
			Public:    true,
		},
	}
}

// evmSpaceApis returns the collection of built-in RPC APIs for EVM space.
func evmSpaceApis(router infuraNode.Router, handler handler.EthHandler) ([]API, error) {
	clientProvider := infuraNode.NewEthClientProvider(router)

	return []API{
		{
			Namespace: "eth",
			Version:   "1.0",
			Service:   newEthAPI(clientProvider, handler),
			Public:    true,
		}, {
			Namespace: "web3",
			Version:   "1.0",
			Service:   newWeb3API(clientProvider),
			Public:    true,
		}, {
			Namespace: "net",
			Version:   "1.0",
			Service:   newNetAPI(clientProvider),
			Public:    true,
		}, {
			Namespace: "trace",
			Version:   "1.0",
			Service:   newEthTraceAPI(clientProvider),
			Public:    false,
		}, {
			Namespace: "parity",
			Version:   "1.0",
			Service:   newParityAPI(clientProvider),
			Public:    false,
		},
	}, nil
}

// nativeSpaceBridgeApis adapts EVM space RPCs to native space RPCs.
func nativeSpaceBridgeApis(ethNodeURL, cfxNodeURL string) ([]API, error) {
	// TODO configure cluster for CFX bridge?
	cfxApi, err := cfxbridge.NewCfxAPI(ethNodeURL, cfxNodeURL)
	if err != nil {
		return nil, err
	}

	traceApi, err := cfxbridge.NewTraceAPI(ethNodeURL)
	if err != nil {
		return nil, err
	}

	return []API{
		{
			Namespace: "cfx",
			Version:   "1.0",
			Service:   cfxApi,
			Public:    true,
		}, {
			Namespace: "trace",
			Version:   "1.0",
			Service:   traceApi,
			Public:    true, // TODO false by default
		},
	}, nil
}
