package node

import (
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

func NewServer() *util.RpcServer {
	return util.MustNewRpcServer("node", map[string]interface{}{
		"node": &api{
			manager:   NewMananger(cfg.URLs),
			wsManager: NewMananger(cfg.WSURLs),
		},
	})
}

type api struct {
	manager   *Manager
	wsManager *Manager // node manager for websocket fullnodes
}

func (api *api) Add(url string) {
	api.manager.Add(url)
}

func (api *api) WsAdd(url string) {
	api.wsManager.Add(url)
}

func (api *api) Remove(url string) {
	api.manager.Remove(url)
}

func (api *api) WsRemove(url string) {
	api.wsManager.Remove(url)
}

// List returns the URL list of all nodes.
func (api *api) List() []string {
	var nodes []string

	for _, n := range api.manager.List() {
		nodes = append(nodes, n.GetNodeURL())
	}

	return nodes
}

// WSList returns the URL list of all websocket full nodes.
func (api *api) WsList() []string {
	var nodes []string

	for _, n := range api.wsManager.List() {
		nodes = append(nodes, n.GetNodeURL())
	}

	return nodes
}

// Route implements the Router interface. It routes the specified key to any node
// and return the node URL.
func (api *api) Route(key hexutil.Bytes) string {
	return api.manager.Route(key)
}

func (api *api) WsRoute(key hexutil.Bytes) string {
	return api.wsManager.Route(key)
}
