package virtualfilter

import (
	"context"
	"encoding/json"
	"math"
	"math/big"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// core space virtual filter
type cfxFilter struct {
	filterBase
	client *sdk.Client
}

func newCfxFilter(fid rpc.ID, typ filterType, client *sdk.Client) *cfxFilter {
	return &cfxFilter{
		client: client,
		filterBase: filterBase{
			id:              fid,
			typ:             typ,
			lastPollingTime: time.Now(),
		},
	}
}

// implements `virtualFilter` interface

func (f *cfxFilter) fetch() (filterChanges, error) {
	return f.client.Filter().GetFilterChanges(f.id)
}

func (f *cfxFilter) uninstall() (bool, error) {
	metricVirtualFilterSession("cfx", f, -1)
	return f.client.Filter().UninstallFilter(f.id)
}

func (f *cfxFilter) nodeName() string {
	return rpcutil.Url2NodeName(f.client.GetNodeURL())
}

func newCfxBlockFilter(client *sdk.Client) (*cfxFilter, error) {
	fid, err := client.Filter().NewBlockFilter()
	if err != nil {
		return nil, err
	}

	f := newCfxFilter(*fid, filterTypeBlock, client)
	metricVirtualFilterSession("cfx", f, 1)
	return f, nil
}

func newCfxPendingTxnFilter(client *sdk.Client) (*cfxFilter, error) {
	fid, err := client.Filter().NewPendingTransactionFilter()
	if err != nil {
		return nil, err
	}

	f := newCfxFilter(*fid, filterTypePendingTxn, client)
	metricVirtualFilterSession("cfx", f, 1)
	return f, nil
}

type cfxLogFilter struct {
	*cfxFilter

	logStore *mysql.VirtualFilterLogStore
	worker   *cfxFilterWorker
	crit     types.LogFilter
}

func newCfxLogFilter(
	vfls *mysql.VirtualFilterLogStore,
	worker *cfxFilterWorker,
	client *sdk.Client,
	crit types.LogFilter,
) (*cfxLogFilter, error) {
	lf := &cfxLogFilter{
		logStore:  vfls,
		worker:    worker,
		crit:      crit,
		cfxFilter: newCfxFilter(rpc.NewID(), filterTypeLog, client),
	}

	if err := worker.accept(lf); err != nil {
		return nil, err
	}

	metricVirtualFilterSession("cfx", lf, 1)
	return lf, nil
}

func (f *cfxLogFilter) uninstall() (bool, error) {
	metricVirtualFilterSession("cfx", f, -1)
	return f.worker.reject(f)
}

func (f *cfxLogFilter) fetch() (filterChanges, error) {
	// get change epochs from filter worker since last polling
	pchanges, err := f.worker.fetchPollingChanges(f.id)
	if err != nil {
		return nil, err
	}

	// distinguish filter epochs missing of event logs due to cache evict
	var missingBlockhashes []string
	bnMin, bnMax := uint64(math.MaxUint64), uint64(0)

	for _, fe := range pchanges.epochs {
		if !fe.reorged() && len(fe.logs) == 0 { // filter epochs missing of event logs
			missingBlockhashes = append(missingBlockhashes, fe.blockHash.String())
			bnMin, bnMax = util.MinUint64(fe.epochNum, bnMin), util.MaxUint64(fe.epochNum, bnMax)
		}
	}

	sload := (len(missingBlockhashes) > 0)
	metrics.Registry.VirtualFilter.StoreQueryPercentage("cfx", f.nodeName(), "mysql").Mark(sload)

	blockLogs := make(map[string][]types.Log, len(missingBlockhashes))
	if len(missingBlockhashes) > 0 { // load missing event logs from db store
		timeoutCtx, cancel := context.WithTimeout(context.Background(), store.TimeoutGetLogs)
		defer cancel()

		startTime := time.Now()
		defer metrics.Registry.VirtualFilter.QueryFilterChanges("cfx", f.nodeName(), "mysql").UpdateSince(startTime)

		sfilter := store.ParseCfxLogFilter(bnMin, bnMax, &f.crit)
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

			var log types.Log
			if err := json.Unmarshal(logs[i].JsonRepr, &log); err != nil {
				return nil, errors.WithMessage(err, "invalid event log json")
			}

			blockLogs[bh] = append(blockLogs[bh], log)
		}
	}

	changeLogs := make([]*types.SubscriptionLog, 0)

	// locate last re-orged index
	var idx int
	for ; idx < len(pchanges.epochs); idx++ {
		if !pchanges.epochs[idx].reorged() {
			break
		}
	}

	if idx > 0 { // append chain-reorg log
		revertTo := pchanges.epochs[idx-1].revertedTo
		reorg := &types.ChainReorg{
			RevertTo: (*hexutil.Big)(new(big.Int).SetUint64(revertTo)),
		}
		changeLogs = append(changeLogs, &types.SubscriptionLog{ChainReorg: reorg})
	}

	for ; idx < len(pchanges.epochs); idx++ { // append normal event logs
		fe := pchanges.epochs[idx]
		logs := fe.logs
		if len(logs) == 0 { // load from store logs
			logs = blockLogs[fe.blockHash.String()]
		}

		logs = filterCfxLogs(logs, &f.crit)
		for i := range logs {
			changeLogs = append(changeLogs, &types.SubscriptionLog{
				Log: &logs[i],
			})
		}
	}

	fc := &types.CfxFilterChanges{
		Type: "log", Logs: changeLogs,
	}

	return fc, nil
}
