package node

import "github.com/conflux-chain/conflux-infura/util"

func NewServer() *util.RpcServer {
	return util.MustNewRpcServer("node", map[string]interface{}{
		"node": &api{NewMananger()},
	})
}

type api struct {
	manager *Manager
}

func (api *api) Add(url string) {
	api.manager.Add(url)
}

func (api *api) Remove(url string) {
	api.manager.Remove(url)
}

// List returns the URL list of all nodes.
func (api *api) List() []string {
	var nodes []string

	for _, n := range api.manager.List() {
		nodes = append(nodes, n.GetNodeURL())
	}

	return nodes
}

// Route implements the Router interface. It routes the specified key to any node
// and return the node URL.
func (api *api) Route(key []byte) string {
	return api.manager.Route(key)
}
