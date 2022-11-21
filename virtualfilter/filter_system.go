package virtualfilter

import (
	"context"
	"time"

	"github.com/Conflux-Chain/confura/node"
	rpc "github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	web3rpc "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

const (
	// filter change polling settings
	pollingInterval         = 1 * time.Second
	maxPollingDelayDuration = 1 * time.Minute
)

// FilterSystem creates proxy log filter to full node, and instantly polls event logs from
// the full node to persist data in db/cache store for high performance and stable log filter
// data retrieval service.
type FilterSystem struct {
	cfg *Config

	// handler to get filter logs from store or full node.
	lhandler *handler.EthLogsApiHandler

	fnProxies     util.ConcurrentMap // node name => *proxyStub
	filterProxies util.ConcurrentMap // filter ID => *proxyStub
}

func NewFilterSystem(lhandler *handler.EthLogsApiHandler, conf *Config) *FilterSystem {
	return &FilterSystem{cfg: conf, lhandler: lhandler}
}

// NewFilter creates a new virtual delegate filter
func (fs *FilterSystem) NewFilter(client *node.Web3goClient, crit *types.FilterQuery) (*web3rpc.ID, error) {
	proxy := fs.loadOrNewFnProxy(client)

	fid, err := proxy.newFilter(crit)
	if err != nil {
		return nil, err
	}

	fs.filterProxies.Store(*fid, proxy)
	return fid, nil
}

// UninstallFilter uninstalls a virtual delegate filter
func (fs *FilterSystem) UninstallFilter(id web3rpc.ID) (bool, error) {
	if v, ok := fs.filterProxies.Load(id); ok {
		fs.filterProxies.Delete(id)
		return v.(*proxyStub).uninstallFilter(id), nil
	}

	return false, nil
}

// Logs returns the matching log entries from the blockchain node or db/cache store.
func (fs *FilterSystem) GetFilterLogs(id web3rpc.ID) ([]types.Log, error) {
	v, ok := fs.filterProxies.Load(id)
	if !ok {
		return nil, errFilterNotFound
	}

	proxy := v.(*proxyStub)
	fctx, ok := proxy.getFilterContext(id)
	if !ok {
		return nil, errFilterNotFound
	}

	w3c, crit := proxy.client, fctx.crit

	flag, ok := rpc.ParseEthLogFilterType(crit)
	if !ok {
		return nil, rpc.ErrInvalidEthLogFilter
	}

	chainId, err := fs.lhandler.GetNetworkId(w3c.Eth)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get chain ID")
	}

	hardforkBlockNum := util.GetEthHardforkBlockNumber(uint64(chainId))

	if err := rpc.NormalizeEthLogFilter(w3c.Client, flag, crit, hardforkBlockNum); err != nil {
		return nil, err
	}

	if err := rpc.ValidateEthLogFilter(flag, crit); err != nil {
		return nil, err
	}

	// return empty directly if filter block range before eSpace hardfork
	if crit.ToBlock != nil && *crit.ToBlock <= hardforkBlockNum {
		return nil, nil
	}

	logs, hitStore, err := fs.lhandler.GetLogs(context.Background(), w3c.Client.Eth, crit)
	metrics.Registry.RPC.StoreHit("eth_getFilterLogs", "store").Mark(hitStore)

	return logs, err
}

// GetFilterChanges returns the matching log entries since last polling, and updates the filter cursor accordingly.
func (fs *FilterSystem) GetFilterChanges(id web3rpc.ID) ([]types.Log, error) {
	// TODO: get matching logs from db/cache
	return nil, errors.New("not supported yet")
}

func (fs *FilterSystem) loadOrNewFnProxy(client *node.Web3goClient) *proxyStub {
	nn := client.NodeName()

	v, _ := fs.fnProxies.LoadOrStoreFn(nn, func(interface{}) interface{} {
		return newProxyStub(fs, client)
	})

	return v.(*proxyStub)
}

// implement `proxyObserver` interface

func (fs *FilterSystem) onEstablished(pctx proxyContext) {}

func (fs *FilterSystem) onClosed(pctx proxyContext) {
	for dfid := range pctx.delegates {
		fs.filterProxies.Delete(dfid)
	}
}
