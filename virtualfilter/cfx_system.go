package virtualfilter

import (
	"encoding/json"
	"time"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// core space virtual filter system
type cfxFilterSystem struct {
	*filterSystemBase
	conf *cfxConfig
}

func newCfxFilterSystem(
	conf *cfxConfig,
	vfls *mysql.VirtualFilterLogStore,
	shutdownCtx cmdutil.GracefulShutdownContext,
) *cfxFilterSystem {
	return &cfxFilterSystem{
		conf:             conf,
		filterSystemBase: newFilterSystemBase(conf.TTL, vfls, shutdownCtx),
	}
}

func (fs *cfxFilterSystem) newBlockFilter(client *sdk.Client) (rpc.ID, error) {
	f, err := newCfxBlockFilter(client)
	if err != nil {
		return nilRpcId, err
	}

	fs.filterMgr.add(f)
	return f.fid(), nil
}

func (fs *cfxFilterSystem) newPendingTransactionFilter(client *sdk.Client) (rpc.ID, error) {
	f, err := newCfxPendingTxnFilter(client)
	if err != nil {
		return nilRpcId, err
	}

	fs.filterMgr.add(f)
	return f.fid(), nil
}

func (fs *cfxFilterSystem) newFilter(client *sdk.Client, crit types.LogFilter) (rpc.ID, error) {
	nodeName := rpcutil.Url2NodeName(client.GetNodeURL())
	worker, _ := fs.workers.LoadOrStoreFn(nodeName, func(k interface{}) interface{} {
		return newCfxFilterWorker(
			fs.conf.MaxFullFilterEpochs, fs, client, fs.shutdownCtx,
		)
	})

	f, err := newCfxLogFilter(fs.logStore, worker.(*cfxFilterWorker), client, crit)
	if err != nil {
		return nilRpcId, err
	}

	fs.filterMgr.add(f)
	return f.fid(), nil
}

func (fs *cfxFilterSystem) getFilterChanges(id rpc.ID) (*types.CfxFilterChanges, error) {
	vf, ok := fs.filterMgr.get(id)
	if !ok {
		return nil, errFilterNotFound
	}

	fc, err := vf.fetch()
	if err != nil {
		return nil, err
	}

	fs.filterMgr.refresh(id)
	return fc.(*types.CfxFilterChanges), nil
}

func (fs *cfxFilterSystem) uninstallFilter(id rpc.ID) (bool, error) {
	if vf, ok := fs.filterMgr.delete(id); ok {
		return vf.uninstall()
	}

	return true, nil
}

// implement `filterWorkerObserver` interface

func (fs *cfxFilterSystem) onPolled(nodeName string, fid rpc.ID, changes filterChanges) error {
	// convert to virtual filter log
	fchanges := changes.(*types.CfxFilterChanges)
	if len(fchanges.Logs) == 0 {
		return nil
	}

	startTime := time.Now()
	defer metrics.Registry.VirtualFilter.PersistFilterChanges("cfx", nodeName, "mysql").UpdateSince(startTime)

	logger := logrus.WithFields(logrus.Fields{
		"fid":      fid,
		"nodeName": nodeName,
	})

	// prepare table partition for insert at first
	partition, newCreated, err := fs.logStore.PreparePartition(string(fid))
	if err != nil {
		logger.WithError(err).Error("Filter system failed to prepare virtual filter partition")
		return err
	}

	if newCreated { // if new partition created, also try to prune limit exceeded archive partitions
		if err := fs.logStore.GC(string(fid)); err != nil {
			logger.WithError(err).Error("Filter system failed to GC virtual filter partitions")
			return err
		}
	}

	vflogs := make([]mysql.VirtualFilterLog, 0, len(fchanges.Logs))
	for _, sublog := range fchanges.Logs {
		if sublog.IsRevertLog() { // skip reorg log
			continue
		}

		vflog, err := convertCfxLogToVirtualFilterLog(sublog.Log)
		if err != nil {
			logger.WithField("log", *sublog.Log).
				WithError(err).
				Error("Filter system failed to convert virtual filter log")
			return errors.WithMessage(err, "failed to convert virtual filter log")
		}

		vflogs = append(vflogs, *vflog)
	}

	// append polled change logs to partition table
	if err := fs.logStore.Append(string(fid), vflogs, partition); err != nil {
		logger.WithField("partition", partition).
			WithError(err).
			Error("Filter system failed to append filter changeds to database")
		return err
	}

	// metric poll size
	pollsize := int64(len(fchanges.Logs))
	metrics.Registry.VirtualFilter.PollOnceSize("cfx", nodeName).Update(pollsize)

	return nil
}

// convert core space event logs to virtual filter logs
func convertCfxLogToVirtualFilterLog(log *types.Log) (*mysql.VirtualFilterLog, error) {
	jdata, err := json.Marshal(log)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to json marshal")
	}

	convertLogTopicFunc := func(log *types.Log, index int) string {
		if index < 0 || index >= len(log.Topics) {
			return ""
		}

		return log.Topics[index].String()
	}

	return &mysql.VirtualFilterLog{
		BlockNumber:     log.EpochNumber.ToInt().Uint64(),
		BlockHash:       log.BlockHash.String(),
		ContractAddress: log.Address.String(),
		Topic0:          convertLogTopicFunc(log, 0),
		Topic1:          convertLogTopicFunc(log, 1),
		Topic2:          convertLogTopicFunc(log, 2),
		Topic3:          convertLogTopicFunc(log, 3),
		LogIndex:        log.LogIndex.ToInt().Uint64(),
		JsonRepr:        jdata,
	}, nil
}

// filterCfxLogs creates a slice of logs matching the given filter criteria.
func filterCfxLogs(logs []types.Log, crit *types.LogFilter) []types.Log {
	ret := make([]types.Log, 0, len(logs))

	for i := range logs {
		if crit.FromEpoch != nil {
			fromEpoch, ok := crit.FromEpoch.ToInt()
			if ok && fromEpoch.Cmp(logs[i].EpochNumber.ToInt()) > 0 {
				continue
			}
		}

		if crit.ToEpoch != nil {
			toEpoch, ok := crit.ToEpoch.ToInt()
			if ok && toEpoch.Cmp(logs[i].EpochNumber.ToInt()) < 0 {
				continue
			}
		}

		if len(crit.Address) > 0 && !util.IncludeCfxLogAddrs(&logs[i], crit.Address) {
			continue
		}

		if len(crit.Topics) > 0 && !util.MatchCfxLogTopics(&logs[i], crit.Topics) {
			continue
		}

		ret = append(ret, logs[i])
	}

	return ret
}
