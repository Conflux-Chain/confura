package rpc

import (
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/rpc/cfxbridge"
	"github.com/conflux-chain/conflux-infura/rpc/handler"
	"github.com/conflux-chain/conflux-infura/util/metrics/service"
	"github.com/conflux-chain/conflux-infura/util/rpc"
	"github.com/openweb3/web3go"
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
	clientProvider *node.CfxClientProvider, gashandler *handler.GasStationHandler, option ...CfxAPIOption,
) []API {
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
			Service:   &posAPI{},
			Public:    true,
		}, {
			Namespace: "trace",
			Version:   "1.0",
			Service:   &traceAPI{},
			Public:    false,
		}, {
			Namespace: service.Namespace,
			Version:   "1.0",
			Service:   &service.MetricsAPI{},
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
func evmSpaceApis(clientProvider *node.EthClientProvider, option ...EthAPIOption) ([]API, error) {
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
			Service:   &ethTraceAPI{},
			Public:    false,
		}, {
			Namespace: "parity",
			Version:   "1.0",
			Service:   parityAPI{},
			Public:    false,
		},
	}, nil
}

// nativeSpaceBridgeApis adapts EVM space RPCs to native space RPCs.
func nativeSpaceBridgeApis(ethNodeURL, cfxNodeURL string) ([]API, error) {
	// TODO configure cluster for CFX bridge?
	eth, err := web3go.NewClient(ethNodeURL)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to connect to eth space")
	}

	ethChainId, err := eth.Eth.ChainId()
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to get chain ID from eth space")
	}
	rpc.HookMiddlewares(eth.Provider(), ethNodeURL, "eth")

	cfx, err := sdk.NewClient(cfxNodeURL)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to connect to cfx space")
	}
	rpc.HookMiddlewares(cfx.Provider(), cfxNodeURL, "cfx")

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
