package virtualfilter

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// evm space virtual filter
type ethFilter struct {
	filterBase
	client *node.Web3goClient
}

func newEthFilter(fid rpc.ID, typ filterType, client *node.Web3goClient) *ethFilter {
	return &ethFilter{
		client: client,
		filterBase: filterBase{
			id:              fid,
			typ:             typ,
			lastPollingTime: time.Now(),
		},
	}
}

// implements `virtualFilter` interface

func (f *ethFilter) fetch() (filterChanges, error) {
	return f.client.Filter.GetFilterChanges(f.id)
}

func (f *ethFilter) uninstall() (bool, error) {
	return f.client.Filter.UninstallFilter(f.id)
}

func (f *ethFilter) nodeName() string {
	return f.client.NodeName()
}

func newEthBlockFilter(client *node.Web3goClient) (*ethFilter, error) {
	fid, err := client.Filter.NewBlockFilter()
	if err != nil {
		return nil, err
	}

	return newEthFilter(*fid, filterTypeBlock, client), nil
}

func newEthPendingTxnFilter(client *node.Web3goClient) (*ethFilter, error) {
	fid, err := client.Filter.NewPendingTransactionFilter()
	if err != nil {
		return nil, err
	}

	return newEthFilter(*fid, filterTypePendingTxn, client), nil
}

type ethLogFilter struct {
	*ethFilter

	logStore *mysql.VirtualFilterLogStore
	worker   *ethFilterWorker
	crit     types.FilterQuery
}

func newEthLogFilter(
	vfls *mysql.VirtualFilterLogStore,
	worker *ethFilterWorker,
	client *node.Web3goClient,
	crit types.FilterQuery,
) (*ethLogFilter, error) {
	lf := &ethLogFilter{
		logStore:  vfls,
		worker:    worker,
		crit:      crit,
		ethFilter: newEthFilter(rpc.NewID(), filterTypeLog, client),
	}

	if err := worker.accept(lf); err != nil {
		return nil, err
	}

	return lf, nil
}

func (f *ethLogFilter) fetch() (filterChanges, error) {
	// get change blocks from filter worker since last polling
	pchanges, err := f.worker.fetchPollingChanges(f.id)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"fid":  f.id,
			"crit": f.crit,
		}).WithError(err).Error("Virtual filter failed to get polling changes from worker")
		return nil, err
	}

	// distinguish filter blocks missing of event logs due to cache evict
	var missingBlockhashes []string
	bnMin, bnMax := uint64(math.MaxUint64), uint64(0)

	for _, fb := range pchanges.blocks {
		if len(fb.logs) == 0 { // filter blocks missing of event logs
			missingBlockhashes = append(missingBlockhashes, fb.blockHash.String())
			bnMin, bnMax = util.MinUint64(fb.blockNum, bnMin), util.MaxUint64(fb.blockNum, bnMax)
		}
	}

	sload := (len(missingBlockhashes) > 0)
	metrics.Registry.VirtualFilter.StoreQueryPercentage("eth", f.nodeName(), "mysql").Mark(sload)

	blockLogs := make(map[string][]types.Log, len(missingBlockhashes))
	if len(missingBlockhashes) > 0 { // load missing event logs from db store
		timeoutCtx, cancel := context.WithTimeout(context.Background(), store.TimeoutGetLogs)
		defer cancel()

		metricTimer := metrics.Registry.VirtualFilter.QueryFilterChanges("eth", f.nodeName(), "mysql")
		defer metricTimer.Update()

		sfilter := store.ParseEthLogFilterRaw(bnMin, bnMax, &f.crit)
		logs, err := f.logStore.GetLogs(timeoutCtx, string(pchanges.fid), sfilter, missingBlockhashes...)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"fid":         f.id,
				"crit":        f.crit,
				"blockHashes": missingBlockhashes,
			}).WithError(err).Error("Virtual filter failed to get filter change logs from db store")
			return nil, err
		}

		for i := range logs {
			bh := logs[i].BlockHash

			var w3log types.Log
			if err := json.Unmarshal(logs[i].JsonRepr, &w3log); err != nil {
				return nil, errors.WithMessage(err, "invalid event log json")
			}

			blockLogs[bh] = append(blockLogs[bh], w3log)
		}
	}

	var changeLogs []types.Log
	for _, fb := range pchanges.blocks {
		logs := fb.logs
		if len(logs) == 0 { // load from store logs
			logs = blockLogs[fb.blockHash.String()]
		}

		logs = filterEthLogs(logs, &f.crit)
		changeLogs = append(changeLogs, logs...)
	}

	return &types.FilterChanges{Logs: changeLogs}, nil
}
