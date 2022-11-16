package virtualfilter

import (
	"context"
	"sync"
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

// FilterSystem creates proxy log filter to fullnode, and instantly polls event logs from
// the full node to persist data in db/cache store for high performance and stable log filter
// data retrieval service.
type FilterSystem struct {
	cfg *Config
	mu  sync.Mutex

	// handler to get filter logs from store or full node
	lhandler *handler.EthLogsApiHandler

	// manager to create proxy filter to full node
	pfman *proxyFilterManager

	// There would be many virtual delegate filters for a single shared proxy filter, therefore
	// we use delegate context to store the mapping relationships etc.
	delegateContexts map[ProxyFilterID]*delegateContext

	// For every virtual delegate filter, we use filter context to store the recent filter status
	// such as current visiting cursor etc.
	filterContexts map[DelegateFilterID]*FilterContext
}

func NewFilterSystem(lhandler *handler.EthLogsApiHandler, conf *Config) *FilterSystem {
	return &FilterSystem{
		cfg:              conf,
		lhandler:         lhandler,
		pfman:            newProxyFilterManager(),
		delegateContexts: make(map[ProxyFilterID]*delegateContext),
		filterContexts:   make(map[DelegateFilterID]*FilterContext),
	}
}

// NewFilter creates a new virtual delegate filter
func (fs *FilterSystem) NewFilter(client *node.Web3goClient, crit *types.FilterQuery) (*web3rpc.ID, error) {
	var dfid *DelegateFilterID

	err := fs.pfman.loadOrNewProxyFilter(client, func(pctx *proxyContext, loaded bool) error {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		if !loaded { // register the delegate context if initial created
			fs.delegateContexts[*pctx.fid] = newDelegateContext(pctx.fid, client)
		}

		dctx, ok := fs.delegateContexts[*pctx.fid]
		if !ok { // delegate context removed already? let client retry
			return errFilterProxyError
		}

		// snapshot the current filter states
		fctx := dctx.snapshotFilterContext(crit)

		// new a virtual delegate filter
		dfid = fs.newVirtualDelegateFilter(fctx)

		// register the delegate filter as guest of the shared proxy filter
		dctx.registerGuest(dfid)

		if !loaded { // start to poll filter changes if initial created
			go fs.poll(dctx)
		}

		return nil
	})

	return dfid, err
}

// UninstallFilter uninstalls a virtual delegate filter
func (fs *FilterSystem) UninstallFilter(id web3rpc.ID) (bool, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fctx, ok := fs.filterContexts[id]
	if !ok { // filter not found?
		return false, nil
	}

	// delete filter context
	delete(fs.filterContexts, id)

	dctx, ok := fs.delegateContexts[fctx.fid]
	if !ok {
		return true, nil
	}

	// deregister from delegate context
	dctx.deregisterGuest(&id)

	// If no guest delegate filters existed anymore for the shared proxy filter,
	// terminate the proxy filter to reclaim resource usage.
	if len(dctx.guestFilters) == 0 {
		dctx.close()
	}

	return true, nil
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

// poll instantly polling filter changes from full node
func (fs *FilterSystem) poll(dctx *delegateContext) {
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	delayedTicks := 0 // delayed ticks until now
	maxDelayedTicks := int(maxPollingDelayDuration / pollingInterval)

	for {
		select {
		// TODO: add context.Done for graceful shutdown?
		case <-ticker.C:
			fchanges, err := dctx.client.Filter.GetFilterChanges(*dctx.fid)
			if err == nil {
				delayedTicks = 0 // reset delayed ticks
				fs.merge(dctx, fchanges)

				continue
			}

			if IsFilterNotFoundError(err) { // proxy filter cleared by the full node?
				fs.clear(dctx)
				return
			}

			// try as many as possible for any other error
			if delayedTicks++; delayedTicks < maxDelayedTicks {
				continue
			}

			logrus.WithFields(logrus.Fields{
				"proxyFilterID": *dctx.fid,
				"delayedTicks":  delayedTicks,
			}).WithError(err).Error("Filter System failed to poll filter changes after too many delays")
			fs.clear(dctx)

			return
		case <-dctx.termCh:
			fs.clear(dctx)
			return
		}
	}
}

func (fs *FilterSystem) merge(dctx *delegateContext, changes *types.FilterChanges) {
	// TODO: merge filter changes to db && cache store
}

// clear clears proxy filter of the specified delegate context
func (fs *FilterSystem) clear(dctx *delegateContext) {
	// uninstall the proxy filter with error ignored
	defer dctx.client.Filter.UninstallFilter(*dctx.fid)

	// remove the shared proxy filter so that new one can be created next time
	fs.pfman.removeProxyFilter(dctx.client)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// delete delegate context
	delete(fs.delegateContexts, *dctx.fid)

	for dfid := range dctx.guestFilters {
		// delete filter context for the guest filter
		delete(fs.filterContexts, dfid)

		// deregister the guest filter
		dctx.deregisterGuest(&dfid)
	}
}

// newVirtualDelegateFilter creates a virtual delegate filter with specified filter context
func (fs *FilterSystem) newVirtualDelegateFilter(fctx *FilterContext) *DelegateFilterID {
	dfid := web3rpc.NewID()
	fs.filterContexts[dfid] = fctx

	return &dfid
}
