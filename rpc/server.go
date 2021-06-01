package rpc

import (
	infuraNode "github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/util"
)

func NewServer(router infuraNode.Router, handler cfxHandler) *util.RpcServer {
	clientProvider := infuraNode.NewClientProvider(router)

	return util.MustNewRpcServer("cfx", map[string]interface{}{
		"cfx":     newCfxAPI(clientProvider, handler),
		"trace":   newTraceAPI(clientProvider),
		"metrics": &metricsAPI{},
	})
}
