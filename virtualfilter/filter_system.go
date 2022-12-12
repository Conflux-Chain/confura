package virtualfilter

import (
	"context"
	"math"
	"time"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
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

	rpcMethodGetFilterLogs = "eth_getFilterLogs"
)

// FilterSystem creates proxy log filter to full node, and instantly polls event logs from
// the full node to persist data in db/cache store for high performance and stable log filter
// data retrieval service.
type FilterSystem struct {
	cfg *Config

	// graceful shutdown context
	shutdownCtx cmdutil.GracefulShutdownContext

	// handler to get filter logs from store or full node
	lhandler *handler.EthLogsApiHandler
	// log store to persist changed logs for more reliability
	logStore *mysql.VirtualFilterLogStore

	fnProxies     util.ConcurrentMap // node name => *proxyStub
	filterProxies util.ConcurrentMap // filter ID => *proxyStub
}

func NewFilterSystem(
	shutdownCtx cmdutil.GracefulShutdownContext,
	vfls *mysql.VirtualFilterLogStore,
	lhandler *handler.EthLogsApiHandler,
	conf *Config,
) *FilterSystem {
	return &FilterSystem{
		shutdownCtx: shutdownCtx, cfg: conf, lhandler: lhandler, logStore: vfls,
	}
}

// NewFilter creates a new virtual delegate filter
func (fs *FilterSystem) NewFilter(client *node.Web3goClient, crit *types.FilterQuery) (*web3rpc.ID, error) {
	proxy := fs.loadOrNewFnProxy(fs.shutdownCtx, client)

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
	proxy, fctx, ok := fs.loadFilterContext(id)
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

	logs, hitStore, err := fs.lhandler.GetLogs(context.Background(), w3c.Client.Eth, crit, rpcMethodGetFilterLogs)
	metrics.Registry.RPC.StoreHit(rpcMethodGetFilterLogs, "store").Mark(hitStore)

	return logs, err
}

// GetFilterChanges returns the matching log entries since last polling, and updates the filter cursor accordingly.
func (fs *FilterSystem) GetFilterChanges(id web3rpc.ID) (*types.FilterChanges, error) {
	proxy, fctx, ok := fs.loadFilterContext(id)
	if !ok {
		return nil, errFilterNotFound
	}

	fcBlocks, err := proxy.getFilterChanges(id)
	if err != nil {
		return nil, filterProxyError(err)
	}

	var missingBlockhashes []string
	bnMin, bnMax := uint64(math.MaxUint64), uint64(0)

	for _, fb := range fcBlocks {
		if len(fb.logs) == 0 { // filter blocks missing of event logs
			missingBlockhashes = append(missingBlockhashes, fb.blockHash.String())
			bnMin, bnMax = util.MinUint64(fb.blockNum, bnMin), util.MaxUint64(fb.blockNum, bnMax)
		}
	}

	blockLogs := make(map[string][]types.Log, len(missingBlockhashes))

	nodeName := proxy.client.NodeName()
	metrics.Registry.VirtualFilter.StoreQueryPercentage(nodeName, "mysql").Mark(len(missingBlockhashes) > 0)

	if len(missingBlockhashes) > 0 { // load missing event logs from db store
		timeoutCtx, cancel := context.WithTimeout(context.Background(), store.TimeoutGetLogs)
		defer cancel()

		metricTimer := metrics.Registry.VirtualFilter.QueryFilterChanges(nodeName, "mysql")
		defer metricTimer.Update()

		sfilter := store.ParseEthLogFilterRaw(bnMin, bnMax, fctx.crit)
		logs, err := fs.logStore.GetLogs(timeoutCtx, string(proxy.fid), sfilter, missingBlockhashes...)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"proxyFullNode": proxy.client.URL,
				"proxyFilterId": proxy.fid,
				"logFilter":     sfilter,
				"blockHashes":   missingBlockhashes,
			}).WithError(err).Error("Virtual filter failed to get change logs from db store")
			return nil, filterProxyError(err)
		}

		for i := range logs {
			bh := logs[i].BlockHash.String()
			blockLogs[bh] = append(blockLogs[bh], logs[i])
		}
	}

	var changeLogs []types.Log
	for _, fb := range fcBlocks {
		if len(fb.logs) == 0 { // load from store
			changeLogs = append(changeLogs, blockLogs[fb.blockHash.String()]...)
		} else {
			changeLogs = append(changeLogs, fb.logs...)
		}
	}

	fchanges := &types.FilterChanges{
		Logs: filterLogs(changeLogs, fctx.crit),
	}

	return fchanges, nil
}

func (fs *FilterSystem) loadFilterContext(id web3rpc.ID) (*proxyStub, *FilterContext, bool) {
	v, ok := fs.filterProxies.Load(id)
	if !ok {
		return nil, nil, false
	}

	proxy := v.(*proxyStub)

	fctx, ok := proxy.getFilterContext(id)
	if !ok {
		return nil, nil, false
	}

	return proxy, fctx, true
}

func (fs *FilterSystem) loadOrNewFnProxy(shutdownCtx cmdutil.GracefulShutdownContext, client *node.Web3goClient) *proxyStub {
	nn := client.NodeName()

	v, _ := fs.fnProxies.LoadOrStoreFn(nn, func(interface{}) interface{} {
		return newProxyStub(shutdownCtx, fs.cfg, fs, client)
	})

	return v.(*proxyStub)
}

// filterLogs creates a slice of logs matching the given criteria.
func filterLogs(logs []types.Log, crit *types.FilterQuery) []types.Log {
	ret := make([]types.Log, 0, len(logs))

	for i := range logs {
		if crit.FromBlock != nil && crit.FromBlock.Int64() >= 0 && uint64(*crit.FromBlock) > logs[i].BlockNumber {
			continue
		}

		if crit.ToBlock != nil && crit.ToBlock.Int64() >= 0 && uint64(*crit.ToBlock) < logs[i].BlockNumber {
			continue
		}

		if len(crit.Addresses) > 0 && !util.IncludeEthLogAddrs(&logs[i], crit.Addresses) {
			continue
		}

		if len(crit.Topics) > 0 && !util.MatchEthLogTopics(&logs[i], crit.Topics) {
			continue
		}

		ret = append(ret, logs[i])
	}

	return ret
}

// implement `proxyObserver` interface

func (fs *FilterSystem) onEstablished(proxy *proxyStub) {
	// prepare partition table for changed logs persistence
	if _, _, err := fs.logStore.PreparePartition(string(proxy.fid)); err != nil {
		logrus.WithFields(logrus.Fields{
			"proxyFullNode": proxy.client.URL,
			"proxyFilterId": proxy.fid,
		}).WithError(err).Error("Filter system failed to prepare virtual filter partition")
	}
}

func (fs *FilterSystem) onClosed(proxy *proxyStub) {
	for dfid := range proxy.delegates {
		fs.filterProxies.Delete(dfid)
	}

	// clean all partition tables for the proxy filter
	if err := fs.logStore.DeletePartitions(string(proxy.fid)); err != nil {
		logrus.WithFields(logrus.Fields{
			"proxyFullNode": proxy.client.URL,
			"proxyFilterId": proxy.fid,
		}).WithError(err).Error("Filter system failed to clean virtual filter partitions")
	}
}

func (fs *FilterSystem) onPolled(proxy *proxyStub, changes *types.FilterChanges) error {
	metricTimer := metrics.Registry.VirtualFilter.PersistFilterChanges(proxy.client.NodeName(), "mysql")
	defer metricTimer.Update()

	// prepare table partition for insert at first
	partition, newCreated, err := fs.logStore.PreparePartition(string(proxy.fid))
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"proxyFullNode": proxy.client.URL,
			"proxyFilterId": proxy.fid,
		}).WithError(err).Error("Filter system failed to prepare virtual filter partition")
		return err
	}

	if newCreated { // if new partition created, also try to prune limit exceeded archive partitions
		if err := fs.logStore.GC(string(proxy.fid)); err != nil {
			logrus.WithFields(logrus.Fields{
				"proxyFullNode": proxy.client.URL,
				"proxyFilterId": proxy.fid,
			}).WithError(err).Error("Filter system failed to GC virtual filter partitions")
		}
	}

	// append polled change logs to partition table
	if err := fs.logStore.Append(string(proxy.fid), changes.Logs, partition); err != nil {
		logrus.WithFields(logrus.Fields{
			"proxyFullNode": proxy.client.URL,
			"proxyFilterId": proxy.fid,
			"partition":     partition,
		}).WithError(err).Error("Filter system failed to append filter changed logs")
		return err
	}

	return nil
}
