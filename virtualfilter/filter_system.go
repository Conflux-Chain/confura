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
	"github.com/sirupsen/logrus"
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

	// There would be many virtual delegate filters for a single shared proxy filter per full node,
	// therefore we use proxy context to store the mapping relationships etc.
	proxyContexts util.ConcurrentMap // node name => *proxyContext
}

func NewFilterSystem(lhandler *handler.EthLogsApiHandler, conf *Config) *FilterSystem {
	return &FilterSystem{cfg: conf, lhandler: lhandler}
}

// NewFilter creates a new virtual delegate filter
func (fs *FilterSystem) NewFilter(client *node.Web3goClient, crit *types.FilterQuery) (*web3rpc.ID, error) {
	pctx := fs.loadOrNewProxyContext(client)

	pctx.mu.Lock()
	defer pctx.mu.Unlock()

	if pctx.fid == nil {
		// establishes a shared universal proxy filter initially
		fid, err := pctx.client.Filter.NewLogFilter(&types.FilterQuery{})
		if err != nil {
			return nil, err
		}

		pctx.fid = fid

		// start to poll filter changes
		go fs.poll(pctx)
	}

	// new a virtual delegate filter
	dfid := web3rpc.NewID()

	// snapshot the current filter states
	pctx.delegateFilters[dfid] = &FilterContext{
		fid: *pctx.fid, cursor: pctx.cur, crit: crit,
	}

	return &dfid, nil
}

// UninstallFilter uninstalls a virtual delegate filter
func (fs *FilterSystem) UninstallFilter(id web3rpc.ID) (bool, error) {
	var found bool

	fs.proxyContexts.Range(func(key, value interface{}) bool {
		pctx := value.(*proxyContext)

		pctx.mu.Lock()
		defer pctx.mu.Unlock()

		if _, found = pctx.delegateFilters[id]; !found {
			return true
		}

		// delete filter context
		delete(pctx.delegateFilters, id)
		return false
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

func (fs *FilterSystem) loadOrNewProxyContext(client *node.Web3goClient) *proxyContext {
	nn := client.NodeName()

	v, _ := fs.proxyContexts.LoadOrStoreFn(nn, func(interface{}) interface{} {
		return newProxyContext(client)
	})

	return v.(*proxyContext)
}

// poll instantly polling filter changes from full node
func (fs *FilterSystem) poll(pctx *proxyContext) {
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	lastPollingTime := time.Now()

	for range ticker.C {
		if fs.gc(pctx) { // proxy filter garbage collected?
			return
		}

		fchanges, err := pctx.client.Filter.GetFilterChanges(*pctx.fid)
		if err == nil {
			// update last polling time
			lastPollingTime = time.Now()

			fs.merge(pctx, fchanges)
			continue
		}

		// Proxy filter removed by the full node? this may be due to full node reboot.
		if IsFilterNotFoundError(err) {
			fs.clear(pctx)
			return
		}

		// try as many times as possible for any other error
		if time.Since(lastPollingTime) < maxPollingDelayDuration {
			continue
		}

		logrus.WithFields(logrus.Fields{
			"proxyFilterID":   *pctx.fid,
			"delayedDuration": time.Since(lastPollingTime),
		}).WithError(err).Error("Filter System failed to poll filter changes after too many delays")

		fs.clear(pctx)
		return
	}
}

func (fs *FilterSystem) merge(pctx *proxyContext, changes *types.FilterChanges) {
	// TODO: merge filter changes to db && cache store
}

// clear clears the shared proxy filter of specified context
func (fs *FilterSystem) clear(pctx *proxyContext, lockfree ...bool) {
	if len(lockfree) == 0 || !lockfree[0] {
		pctx.mu.Lock()
		defer pctx.mu.Unlock()
	}

	// uninstall the proxy filter with error ignored
	if pctx.fid != nil {
		pctx.client.Filter.UninstallFilter(*pctx.fid)
	}

	// reset the proxy context for reuse
	pctx.fid, pctx.cur = nil, nil
	pctx.delegateFilters = make(map[DelegateFilterID]*FilterContext)
}

// gc clear the proxy filter if not used by any delegate filter
func (fs *FilterSystem) gc(pctx *proxyContext) bool {
	pctx.mu.Lock()
	defer pctx.mu.Unlock()

	if len(pctx.delegateFilters) > 0 {
		return false
	}

	// If no delegate filters existed anymore for the shared proxy filter,
	// terminate the proxy filter to reclaim resource usage.
	fs.clear(pctx, true)
	return true
}
