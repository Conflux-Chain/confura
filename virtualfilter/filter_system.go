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
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	web3rpc "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	filterChangesPollingInterval       = 1 * time.Second
	filterChangesPollingMaxErrTryTimes = 5
)

var (
	// errFilterProxyError uniform error returned to the end user when proxy filter error.
	errFilterProxyError = errors.New("filter proxy error")
)

// FilterSystem creates proxy log filter to fullnode, and instantly polls event logs from
// the full node to persist data in db/cache store for high performance and stable log filter
// data retrieval service.
type FilterSystem struct {
	cfg *Config
	mu  sync.Mutex

	// handler to get filter logs from store or full node
	lhandler *handler.EthLogsApiHandler

	// node name => delegate client
	delegateClients util.ConcurrentMap
	// proxy filter ID => delegate context
	delegateContexts map[web3rpc.ID]*delegateContext
	// proxy filter ID => hashset of delegate filter IDs ahead of the proxy filter
	delegateFilters map[web3rpc.ID]map[web3rpc.ID]struct{}
	// delegate filter ID => filter context
	filterContexts map[web3rpc.ID]*FilterContext
}

func NewFilterSystem(lhandler *handler.EthLogsApiHandler, conf *Config) *FilterSystem {
	return &FilterSystem{
		cfg:              conf,
		lhandler:         lhandler,
		delegateContexts: make(map[web3rpc.ID]*delegateContext),
		delegateFilters:  make(map[web3rpc.ID]map[web3rpc.ID]struct{}),
		filterContexts:   make(map[web3rpc.ID]*FilterContext),
	}
}

// NewFilter delegated to create new proxy filter.
func (fs *FilterSystem) NewFilter(client *node.Web3goClient, crit *types.FilterQuery) (*web3rpc.ID, error) {
	dclient := fs.loadOrNewDelegateClient(client)

	fid, newCreated, err := dclient.NewFilter(crit)
	if err != nil {
		return nil, errFilterProxyError
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	if newCreated {
		fs.delegateContexts[*fid] = newDelegateContext(fid, dclient)
	}

	dctx, ok := fs.delegateContexts[*fid]
	if !ok { // delegate context already removed? let client retry
		return nil, errFilterProxyError
	}

	fctx := dctx.snapshotFilterContext(crit)
	dfid := web3rpc.NewID()
	fs.filterContexts[dfid] = fctx

	if _, ok := fs.delegateFilters[*fid]; !ok {
		fs.delegateFilters[*fid] = make(map[web3rpc.ID]struct{})
	}
	fs.delegateFilters[*fid][dfid] = struct{}{}

	if newCreated {
		// start to poll filter changes for the new proxy filter
		go fs.pollFilterChanges(dctx)
	}

	return &dfid, nil
}

// delegateUninstallFilter uninstalls a delegate filter.
func (fs *FilterSystem) UninstallFilter(id web3rpc.ID) (bool, error) {
	fctx, ok := fs.delFilter(id)
	if !ok {
		return false, nil
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	if len(fs.delegateFilters[fctx.fid]) > 0 {
		return true, nil
	}

	// If no delegate filters existed for the proxy filter, terminate the proxy filter
	// to reclaim resource usage.
	if dctx, ok := fs.delegateContexts[fctx.fid]; ok {
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

func (fs *FilterSystem) loadOrNewDelegateClient(client *node.Web3goClient) *delegateClient {
	nodeName := rpcutil.Url2NodeName(client.URL)

	v, _ := fs.delegateClients.LoadOrStoreFn(nodeName, func(k interface{}) interface{} {
		return &delegateClient{Web3goClient: client}
	})

	return v.(*delegateClient)
}

// pollFilterChanges instantly polls filter changes from full node
func (fs *FilterSystem) pollFilterChanges(dctx *delegateContext) {
	ticker := time.NewTicker(filterChangesPollingInterval)
	defer ticker.Stop()

	for tryErr := 0; ; {
		select {
		case <-ticker.C:
			fchanges, err := dctx.client.Filter.GetFilterChanges(*dctx.fid)
			if err == nil {
				tryErr = 0
				fs.mergeFilterChanges(dctx, fchanges)
				continue
			}

			if IsFilterNotFoundError(err) { // proxy filter cleared by the full node?
				fs.clear(dctx)
				return
			}

			// try as many as possible times for any other error
			if tryErr++; tryErr >= filterChangesPollingMaxErrTryTimes {
				logrus.WithField("proxyFilterID", *dctx.fid).
					WithError(err).
					Error("Filter System failed to poll filter changes after too many retries")

				fs.clear(dctx)
				return
			}
		case <-dctx.termCh:
			fs.clear(dctx)
			return
		default:
			// TODO: add context.Done for graceful shutdown?
		}
	}
}

// deleteFilter deletes delegate filter of specified filter ID
func (fs *FilterSystem) delFilter(id web3rpc.ID) (*FilterContext, bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fctx, ok := fs.filterContexts[id]
	if !ok { // filter not found?
		return nil, false
	}

	// delete filter context
	delete(fs.filterContexts, id)

	// delete delegate filter
	delete(fs.delegateFilters[fctx.fid], id)

	return fctx, true
}

// clear clears proxy filter of specified delegate context, also resets the delegate client
// so new proxy filter could be created for the next time.
func (fs *FilterSystem) clear(dctx *delegateContext) {
	// uninstall the filter with error ignored
	defer dctx.client.Filter.UninstallFilter(*dctx.fid)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// reset client to allow new singleton proxy filter
	dctx.client.reset()

	// delete delegate context
	delete(fs.delegateContexts, *dctx.fid)

	// delete all filter contexts under this delegate context
	delegateFilters := fs.delegateFilters[*dctx.fid]
	for dfid := range delegateFilters {
		delete(fs.filterContexts, dfid)
	}

	// delete delegate filters
	delete(fs.delegateFilters, *dctx.fid)
}

func (fs *FilterSystem) mergeFilterChanges(dctx *delegateContext, changes *types.FilterChanges) {
	// TODO: merge filter changes to db && cache store
}
