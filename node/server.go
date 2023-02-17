package node

import (
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/rpc"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// NewServer creates node management RPC server
func NewServer(db *mysql.MysqlStore, nf nodeFactory, groupConf map[Group]UrlConfig) *rpc.Server {
	pool := newNodePool(db, nf, groupConf)

	return rpc.MustNewServer("node", map[string]interface{}{
		"node": &api{pool: pool},
	})
}

// api node management RPC APIs.
type api struct {
	pool *nodePool
}

func (api *api) Add(group Group, url string) {
	if m, ok := api.pool.GetManager(group); ok {
		m.Add(url)
	}
}

func (api *api) Remove(group Group, url string) {
	if m, ok := api.pool.GetManager(group); ok {
		m.Remove(url)
	}
}

// List returns the URL list of all nodes.
func (api *api) List(group Group) []string {
	m, ok := api.pool.GetManager(group)
	if !ok {
		return nil
	}

	var nodes []string

	for _, n := range m.List() {
		nodes = append(nodes, n.Url())
	}

	return nodes
}

func (api *api) Status(group Group, url *string) (res []Status) {
	mgr, ok := api.pool.GetManager(group)
	if !ok { // no group found
		return
	}

	if url != nil { // get specific node status
		if n := mgr.Get(*url); !util.IsInterfaceValNil(n) {
			res = append(res, n.Status())
		}

		return
	}

	// get all group node status
	for _, n := range mgr.List() {
		res = append(res, n.Status())
	}

	return
}

// List returns the URL list of all nodes.
func (api *api) ListAll() map[Group][]string {
	result := make(map[Group][]string)

	for group := range api.pool.AllManagers() {
		result[Group(group)] = api.List(Group(group))
	}

	return result
}

// Route implements the Router interface. It routes the specified key to any node
// and return the node URL.
func (api *api) Route(group Group, key hexutil.Bytes) string {
	if m, ok := api.pool.GetManager(group); ok {
		return m.Route(key)
	}

	return ""
}
