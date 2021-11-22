package rpc

import (
	infuraNode "github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
)

const (
	infuraRpcServerName = "infura_rpcsrv"
)

// API describes the set of methods offered over the RPC interface
type API struct {
	Namespace string      // namespace under which the rpc methods of Service are exposed
	Version   string      // api version for DApp's
	Service   interface{} // receiver instance which holds the methods
	Public    bool        // indication if the methods must be considered safe for public use
}

// MustNewServer new RPC server by specifying router, handler and exposed modules.
// Argument exposedModules is a list of API modules to expose via the RPC interface.
// If the module list is empty, all RPC API endpoints designated public will be
// exposed.
func MustNewServer(router infuraNode.Router, handler cfxHandler, exposedModules []string) *util.RpcServer {
	allApis := apis(router, handler) // retrieve all available RPC apis
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
		return util.MustNewRpcServer(infuraRpcServerName, servedApis)
	}

	filteredApis := make(map[string]interface{}, len(exposedModules))
	for _, m := range exposedModules {
		if svc, ok := servedApis[m]; ok {
			filteredApis[m] = svc
		} else {
			logrus.WithField("module", m).Fatal("Failed to new RPC server with unknown exposed module")
		}
	}

	return util.MustNewRpcServer(infuraRpcServerName, filteredApis)
}

// apis returns the collection of built-in RPC APIs.
func apis(router infuraNode.Router, handler cfxHandler) []API {
	clientProvider := infuraNode.NewClientProvider(router)

	return []API{
		{
			Namespace: "cfx",
			Version:   "1.0",
			Service:   newCfxAPI(clientProvider, handler),
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
		},
	}
}
