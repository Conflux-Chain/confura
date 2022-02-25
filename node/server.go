package node

import (
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

func NewServer() *util.RpcServer {
	managers := make(map[Group]*Manager)
	for k, v := range urlCfg {
		managers[k] = NewManager(v.Nodes)
	}

	return util.MustNewRpcServer("node", map[string]interface{}{
		"node": &api{managers},
	})
}

type api struct {
	managers map[Group]*Manager
}

func (api *api) Add(group Group, url string) {
	if m, ok := api.managers[group]; ok {
		m.Add(url)
	}
}

func (api *api) Remove(group Group, url string) {
	if m, ok := api.managers[group]; ok {
		m.Remove(url)
	}
}

// List returns the URL list of all nodes.
func (api *api) List(group Group) []string {
	m, ok := api.managers[group]
	if !ok {
		return nil
	}

	var nodes []string

	for _, n := range m.List() {
		nodes = append(nodes, n.GetNodeURL())
	}

	return nodes
}

// Route implements the Router interface. It routes the specified key to any node
// and return the node URL.
func (api *api) Route(group Group, key hexutil.Bytes) string {
	if m, ok := api.managers[group]; ok {
		return m.Route(key)
	}

	return ""
}
