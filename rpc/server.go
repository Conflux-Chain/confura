package rpc

import (
	infuraNode "github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
)

func NewServer(router infuraNode.Router, db store.Store) *util.RpcServer {
	clientProvider := infuraNode.NewClientProvider(router)

	return util.MustNewRpcServer("cfx", map[string]interface{}{
		"cfx":     newCfxAPI(clientProvider, db),
		"trace":   newTraceAPI(clientProvider),
		"metrics": &metricsAPI{},
	})
}
