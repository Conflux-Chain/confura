package virtualfilter

import (
	"encoding/json"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// evm space virtual filter system
type ethFilterSystem struct {
	*filterSystemBase
	conf *ethConfig
}

func newEthFilterSystem(
	conf *ethConfig,
	vfls *mysql.VirtualFilterLogStore,
	shutdownCtx cmdutil.GracefulShutdownContext,
) *ethFilterSystem {
	return &ethFilterSystem{
		conf:             conf,
		filterSystemBase: newFilterSystemBase(conf.TTL, vfls, shutdownCtx),
	}
}

func (fs *ethFilterSystem) newBlockFilter(client *node.Web3goClient) (rpc.ID, error) {
	f, err := newEthBlockFilter(client)
	if err != nil {
		return nilRpcId, err
	}

	fs.filterMgr.add(f)
	return f.fid(), nil
}

func (fs *ethFilterSystem) newPendingTransactionFilter(client *node.Web3goClient) (rpc.ID, error) {
	f, err := newEthPendingTxnFilter(client)
	if err != nil {
		return nilRpcId, err
	}

	fs.filterMgr.add(f)
	return f.fid(), nil
}

func (fs *ethFilterSystem) newFilter(client *node.Web3goClient, crit types.FilterQuery) (rpc.ID, error) {
	worker, _ := fs.workers.LoadOrStoreFn(client.NodeName(), func(k interface{}) interface{} {
		return newEthFilterWorker(
			fs.conf.MaxFullFilterBlocks, fs, client, fs.shutdownCtx,
		)
	})

	f, err := newEthLogFilter(fs.logStore, worker.(*ethFilterWorker), client, crit)
	if err != nil {
		return nilRpcId, err
	}

	fs.filterMgr.add(f)
	return f.fid(), nil
}

func (fs *ethFilterSystem) getFilterChanges(id rpc.ID) (*types.FilterChanges, error) {
	vf, ok := fs.filterMgr.get(id)
	if !ok {
		return nil, errFilterNotFound
	}

	fc, err := vf.fetch()
	if err != nil {
		return nil, err
	}

	fs.filterMgr.refresh(id)
	return fc.(*types.FilterChanges), nil
}

func (fs *ethFilterSystem) uninstallFilter(id rpc.ID) (bool, error) {
	if vf, ok := fs.filterMgr.delete(id); ok {
		return vf.uninstall()
	}

	return true, nil
}

// implement `filterWorkerObserver` interface

func (fs *ethFilterSystem) onPolled(nodeName string, fid rpc.ID, changes filterChanges) error {
	metricTimer := metrics.Registry.VirtualFilter.PersistFilterChanges("eth", nodeName, "mysql")
	defer metricTimer.Update()

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

	// convert to virtual filter log
	fchanges := changes.(*types.FilterChanges)
	vflogs := make([]mysql.VirtualFilterLog, 0, len(fchanges.Logs))

	for i := range fchanges.Logs {
		vflog, err := convertEthLogToVirtualFilterLog(&fchanges.Logs[i])
		if err != nil {
			logger.WithField("log", fchanges.Logs[i]).
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
	metrics.Registry.VirtualFilter.PollOnceSize("eth", nodeName).Update(pollsize)

	return nil
}

// convert evm space event logs to virtual filter logs
func convertEthLogToVirtualFilterLog(log *types.Log) (*mysql.VirtualFilterLog, error) {
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
		BlockNumber:     log.BlockNumber,
		BlockHash:       log.BlockHash.String(),
		ContractAddress: log.Address.String(),
		Topic0:          convertLogTopicFunc(log, 0),
		Topic1:          convertLogTopicFunc(log, 1),
		Topic2:          convertLogTopicFunc(log, 2),
		Topic3:          convertLogTopicFunc(log, 3),
		LogIndex:        uint64(log.Index),
		JsonRepr:        jdata,
	}, nil
}

// filterEthLogs creates a slice of logs matching the given criteria.
func filterEthLogs(logs []types.Log, crit *types.FilterQuery) []types.Log {
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
