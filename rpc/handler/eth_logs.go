package handler

import (
	"context"
	"time"

	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/store/mysql"
	"github.com/openweb3/web3go"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type EthLogsApiHandler struct {
	storeHandler *EthStoreHandler // chained store handlers
	ms           *mysql.MysqlStore

	v2 *EthLogsApiHandlerV2
}

func NewEthLogsApiHandler(sh *EthStoreHandler, ms *mysql.MysqlStore) *EthLogsApiHandler {
	var v2 *EthLogsApiHandlerV2
	if ms.Config().AddressIndexedLogEnabled {
		v2 = NewEthLogsApiHandlerV2(ms)
	}

	return &EthLogsApiHandler{
		storeHandler: sh, ms: ms, v2: v2,
	}
}

func (h *EthLogsApiHandler) GetLogs(
	ctx context.Context, w3c *web3go.Client, filter web3Types.FilterQuery) ([]web3Types.Log, bool, error) {
	if h.v2 != nil {
		return h.v2.GetLogs(ctx, w3c.Eth, &filter)
	}

	if filter.BlockHash != nil { // convert block hash to block number
		block, err := w3c.Eth.BlockByHash(*filter.BlockHash, false)
		if err == nil && block == nil {
			err = errors.New("unknown block")
		}

		if err != nil {
			return nil, false, errors.WithMessage(err, "invalid block hash")
		}

		fromBn := web3Types.BlockNumber(block.Number.Int64())

		filter.BlockHash = nil
		filter.FromBlock = &fromBn
		filter.ToBlock = &fromBn
	}

	return h.getLogsByRange(ctx, w3c, &filter)
}

func (h *EthLogsApiHandler) getLogsByRange(ctx context.Context, w3c *web3go.Client, filter *web3Types.FilterQuery) ([]web3Types.Log, bool, error) {
	start := time.Now()

	// record the reorg version before query to ensure data consistence
	lastReorgVersion, err := h.ms.GetReorgVersion()
	if err != nil {
		return nil, false, err
	}

	for {
		logs, hitStore, err := h.getLogsReorgGuard(ctx, w3c, filter)
		if err != nil {
			return nil, false, err
		}

		// check the reorg version after query
		reorgVersion, err := h.ms.GetReorgVersion()
		if err != nil {
			return nil, false, err
		}

		if reorgVersion == lastReorgVersion {
			return logs, hitStore, nil
		}

		logrus.WithFields(logrus.Fields{
			"lastReorgVersion": lastReorgVersion,
			"reorgVersion":     reorgVersion,
		}).Debug("Reorg detected, retry query eSpace event logs")

		// when reorg occurred, check timeout before retry.
		if time.Since(start) > store.TimeoutGetLogs {
			return nil, false, store.ErrGetLogsTimeout
		}

		// reorg version changed during data query and try again.
		lastReorgVersion = reorgVersion
	}
}

func (h *EthLogsApiHandler) getLogsReorgGuard(ctx context.Context, w3c *web3go.Client, filter *web3Types.FilterQuery) ([]web3Types.Log, bool, error) {
	// try to query event logs from database and fullnode
	dbFilter, fnFilter, err := h.splitLogFilter(w3c, filter)
	if err != nil {
		return nil, false, err
	}

	var logs []web3Types.Log

	// query data from database
	if dbFilter != nil {
		chainId, err := w3c.Eth.ChainId()
		if err != nil {
			return nil, false, errors.WithMessage(err, "failed to get chain id")
		}

		sfilter, ok := store.ParseEthLogFilter(w3c, uint32(*chainId), dbFilter)
		if !ok {
			return nil, false, store.ErrUnsupported
		}

		dbLogs, err := h.storeHandler.GetLogs(ctx, *sfilter)
		if err != nil {
			return nil, false, err
		}

		logrus.WithFields(logrus.Fields{
			"fromBlock": *dbFilter.FromBlock,
			"toBlock":   *dbFilter.ToBlock,
			"dbFilter":  dbFilter,
		}).Debug("Loaded eSpace event logs from store")

		logs = append(logs, dbLogs...)
	}

	// query data from fullnode
	if fnFilter != nil {
		fnLogs, err := w3c.Eth.Logs(*fnFilter)
		if err != nil {
			return nil, false, err
		}

		logrus.WithFields(logrus.Fields{
			"fromBlock": *fnFilter.FromBlock,
			"toBlock":   *fnFilter.ToBlock,
			"fnFilter":  fnFilter,
		}).Debug("Loaded eSpace event logs from fullnode")

		logs = append(logs, fnLogs...)
	}

	// will remove Limit filter in future, and do not care about perf now.
	if filter.Limit != nil && len(logs) > int(*filter.Limit) {
		logs = logs[len(logs)-int(*filter.Limit):]
	}

	return logs, dbFilter != nil, nil
}

// splitLogFilter splits log filter into 2 parts to query data from database or fullnode.
func (h *EthLogsApiHandler) splitLogFilter(w3c *web3go.Client, filter *web3Types.FilterQuery) (*web3Types.FilterQuery, *web3Types.FilterQuery, error) {
	minBlock, maxBlock, err := h.ms.GetGlobalEpochRange()
	if err != nil {
		return nil, nil, err
	}

	chainId, err := w3c.Eth.ChainId()
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed to get chain id")
	}

	sfilter, ok := store.ParseEthLogFilter(w3c, uint32(*chainId), filter)
	if !ok || sfilter.BlockRange == nil {
		return nil, nil, store.ErrUnsupported
	}

	blockFrom, blockTo := sfilter.BlockRange.From, sfilter.BlockRange.To

	logger := logrus.WithFields(logrus.Fields{
		"sfilter": sfilter, "minBlock": minBlock, "maxBlock": maxBlock,
	})

	// some data pruned
	if blockFrom < minBlock {
		logger.Debug("Some eSpace event logs already pruned")
		return nil, nil, store.ErrAlreadyPruned
	}

	// all data in database
	if blockTo <= maxBlock {
		logger.Debug("All eSpace event logs in database")
		return filter, nil, nil
	}

	metrics.GetOrRegisterHistogram(nil, "rpc/eth_getLogs/split/fn").Update(int64(blockTo - maxBlock))

	// no data in database
	if blockFrom > maxBlock {
		logger.Debug("All eSpace event logs are not in database")
		return nil, filter, nil
	}

	// otherwise, partial data in databse
	dbFilter, fnFilter := *filter, *filter

	dbMaxBlock := web3Types.BlockNumber(maxBlock)
	dbFilter.ToBlock = &dbMaxBlock

	fnMinBlock := web3Types.BlockNumber(maxBlock + 1)
	fnFilter.FromBlock = &fnMinBlock

	logger.WithFields(logrus.Fields{
		"dbMaxBlock": dbMaxBlock, "fnMinBlock": fnMinBlock,
	}).Debug("eSpace event logs are partially in database")

	return &dbFilter, &fnFilter, nil
}
