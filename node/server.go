package node

import (
	"sync"

	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/rpc"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	errDbNotAvailableForPersistence = errors.New("db not available for persistence")
)

// MustNewServer creates node management RPC server
func MustNewServer(db *mysql.MysqlStore, nf nodeFactory, grpConf map[Group]UrlConfig) *rpc.Server {
	npool := newNodePool(nf)

	if db != nil {
		// load node route group config from db
		routeGroups, err := db.LoadNodeRouteGroups()
		if err != nil {
			logrus.WithError(err).Fatal("Failed to load node route groups from db")
		}

		// merge node route groups with the pre-defined config
		for _, grp := range routeGroups {
			grpConf[Group(grp.Name)] = UrlConfig{Nodes: grp.Nodes}
		}
	}

	// add route group nodes to the node pool
	for grp, cfg := range grpConf {
		if err := npool.add(grp, cfg.Nodes...); err != nil {
			logrus.WithFields(logrus.Fields{
				"group":  grp,
				"config": cfg,
			}).WithError(err).Fatal("Failed to add group nodes to the pool")
		}
	}

	return rpc.MustNewServer("node", map[string]interface{}{
		"node": &api{h: &apiHandler{dbs: db, pool: npool}},
	})
}

// api node management RPC APIs.
type api struct {
	h *apiHandler
}

func (api *api) Add(group Group, url string, saveGrps ...bool) error {
	var saveGrp bool
	if len(saveGrps) > 0 {
		saveGrp = saveGrps[0]
	}

	return api.h.addGroupNode(group, url, saveGrp)
}

func (api *api) Remove(group Group, url string, saveGrps ...bool) error {
	var saveGrp bool
	if len(saveGrps) > 0 {
		saveGrp = saveGrps[0]
	}

	return api.h.delGroupNode(group, url, saveGrp)
}

// List returns the URL list of all nodes.
func (api *api) List(group Group) []string {
	return api.h.pool.get(group)
}

func (api *api) Status(group Group, urls ...string) (res []Status) {
	return api.h.pool.status(group, urls...)
}

// ListAll returns the URL list of all nodes by group
func (api *api) ListAll() map[Group][]string {
	res := make(map[Group][]string)

	for _, grp := range api.h.pool.groups() {
		res[grp] = api.List(grp)
	}

	return res
}

// Route implements the Router interface. It routes the specified key to any node
// and return the node URL.
func (api *api) Route(group Group, key hexutil.Bytes) string {
	if m, ok := api.h.pool.manager(group); ok {
		return m.Route(key)
	}

	return ""
}

// apiHandler rpc handler for node api
type apiHandler struct {
	mu sync.Mutex

	// node pool
	pool *nodePool
	// db store to save node route configs
	dbs *mysql.MysqlStore
}

func (h *apiHandler) addGroupNode(grp Group, url string, saveGrp bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !saveGrp { // in-memory update only
		return h.pool.add(grp, url)
	}

	if h.dbs == nil { // db is not available for update
		return errDbNotAvailableForPersistence
	}

	if err := h.pool.add(grp, url); err != nil {
		return err
	}

	routeGroup := &mysql.NodeRouteGroup{
		Name:  string(grp),
		Nodes: dedupNodeUrls(h.pool.get(grp)),
	}

	if err := h.dbs.StoreNodeRouteGroup(routeGroup); err != nil {
		h.pool.del(grp, url) // revert in-memory update
		return err
	}

	return nil
}

func (h *apiHandler) delGroupNode(grp Group, url string, saveGrp bool) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !saveGrp { // in-memory update only
		h.pool.del(grp, url)
		return nil
	}

	if h.dbs == nil { // db is not available for update
		return errDbNotAvailableForPersistence
	}

	updateRtGrp := &mysql.NodeRouteGroup{
		Name:  string(grp),
		Nodes: dedupNodeUrls(h.pool.get(grp, url)),
	}

	if len(updateRtGrp.Nodes) > 0 { // update node set for the route group
		err = h.dbs.StoreNodeRouteGroup(updateRtGrp)
	} else { // delete node route group since no nodes will be left after delete
		err = h.dbs.DelNodeRouteGroup(updateRtGrp.Name)
	}

	if err == nil {
		h.pool.del(grp, url)
	}

	return err
}
