package virtualfilter

import (
	"context"
	"time"

	"github.com/Conflux-Chain/confura/node"
	rpc "github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	lru "github.com/hashicorp/golang-lru"
	web3rpc "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

const (
	// filter change polling settings
	pollingInterval         = 1 * time.Second
	maxPollingDelayDuration = 1 * time.Minute

	proxyCacheSize = 5000
)

// FilterSystem creates proxy log filter to full node, and instantly polls event logs from
// the full node to persist data in db/cache store for high performance and stable log filter
// data retrieval service.
type FilterSystem struct {
	cfg *Config

	// handler to get filter logs from store or full node.
	lhandler *handler.EthLogsApiHandler

	fnProxies        util.ConcurrentMap // node name => *proxyStub
	filterProxyCache *lru.Cache         // filter ID => *proxyStub
}

func NewFilterSystem(lhandler *handler.EthLogsApiHandler, conf *Config) *FilterSystem {
	cache, _ := lru.New(proxyCacheSize)

	return &FilterSystem{
		cfg: conf, lhandler: lhandler, filterProxyCache: cache,
	}
}

// NewFilter creates a new virtual delegate filter
func (fs *FilterSystem) NewFilter(client *node.Web3goClient, crit *types.FilterQuery) (*web3rpc.ID, error) {
	proxy := fs.loadOrNewFnProxy(client)

	fid, err := proxy.newFilter(crit)
	if err != nil {
		return nil, err
	}

	fs.filterProxyCache.Add(*fid, proxy)
	return fid, nil
}

// UninstallFilter uninstalls a virtual delegate filter
func (fs *FilterSystem) UninstallFilter(id web3rpc.ID) (bool, error) {
	if v, ok := fs.filterProxyCache.Get(id); ok {
		fs.filterProxyCache.Remove(id)
		return v.(*proxyStub).uninstallFilter(&id), nil
	}

	var found bool
	fs.fnProxies.Range(func(k, v interface{}) bool {
		found = v.(*proxyStub).uninstallFilter(&id)
		return !found
	})

	return found, nil
}

// Logs returns the matching log entries from the blockchain node or db/cache store.
func (fs *FilterSystem) GetFilterLogs(w3c *node.Web3goClient, crit *types.FilterQuery) ([]types.Log, error) {
	flag, ok := rpc.ParseEthLogFilterType(crit)
	if !ok {
		return ethEmptyLogs, rpc.ErrInvalidEthLogFilter
	}

	chainId, err := fs.lhandler.GetNetworkId(w3c.Eth)
	if err != nil {
		return ethEmptyLogs, errors.WithMessage(err, "failed to get chain ID")
	}

	hardforkBlockNum := util.GetEthHardforkBlockNumber(uint64(chainId))

	if err := rpc.NormalizeEthLogFilter(w3c.Client, flag, crit, hardforkBlockNum); err != nil {
		return ethEmptyLogs, err
	}

	if err := rpc.ValidateEthLogFilter(flag, crit); err != nil {
		return ethEmptyLogs, err
	}

	// return empty directly if filter block range before eSpace hardfork
	if crit.ToBlock != nil && *crit.ToBlock <= hardforkBlockNum {
		return ethEmptyLogs, nil
	}

	logs, hitStore, err := fs.lhandler.GetLogs(context.Background(), w3c.Client.Eth, crit)
	metrics.Registry.RPC.StoreHit("eth_getFilterLogs", "store").Mark(hitStore)

	if logs == nil { // uniform empty logs
		logs = ethEmptyLogs
	}

	return logs, err
}

// GetFilterChanges returns the matching log entries since last polling, and updates the filter cursor accordingly.
func (fs *FilterSystem) GetFilterChanges(w3c *node.Web3goClient, crit *types.FilterQuery) ([]types.Log, error) {
	// TODO: get matching logs from db/cache
	return nil, errors.New("not supported yet")
}

func (fs *FilterSystem) loadOrNewFnProxy(client *node.Web3goClient) *proxyStub {
	nn := client.NodeName()

	v, _ := fs.fnProxies.LoadOrStoreFn(nn, func(interface{}) interface{} {
		return newProxyStub(client)
	})

	return v.(*proxyStub)
}
