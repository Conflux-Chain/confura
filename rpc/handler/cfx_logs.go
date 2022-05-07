package handler

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/store/mysql"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
)

var (
	errEventLogsTooStale = errors.New("event logs are too stale (already pruned)")
)

type CfxLogsApiHandler struct {
	storeHandler  CfxStoreHandler       // chained store handlers
	prunedHandler *CfxPrunedLogsHandler // pruned event logs handler

	// errors that are intercepted from fullnode delegation
	// err => whether op hits in the store
	interceptableErrMap map[error]bool

	ms *mysql.MysqlStore
}

func NewCfxLogsApiHandler(sh CfxStoreHandler, ph *CfxPrunedLogsHandler, ms *mysql.MysqlStore) *CfxLogsApiHandler {
	return &CfxLogsApiHandler{
		storeHandler:  sh,
		prunedHandler: ph,
		interceptableErrMap: map[error]bool{
			store.ErrGetLogsResultSetTooLarge: true,
			store.ErrGetLogsTimeout:           false,
		},
		ms: ms,
	}
}

func (h *CfxLogsApiHandler) GetLogs(
	ctx context.Context, cfx sdk.ClientOperator, filter *types.LogFilter) ([]types.Log, bool, error) {
	if len(filter.BlockHashes) > 1 {
		return h.getLogsByBlockHashes(ctx, cfx, filter)
	}

	if len(filter.BlockHashes) == 1 {
		return h.getLogsBySingleBlockHash(ctx, cfx, filter)
	}

	return h.getLogsByRange(ctx, cfx, filter)
}

func (h *CfxLogsApiHandler) getLogsByBlockHashes(
	ctx context.Context, cfx sdk.ClientOperator, originFilter *types.LogFilter) ([]types.Log, bool, error) {
	var blockNumbers []int
	var hitStore bool
	blockNumber2HashCache := make(map[int]types.Hash)

	// Get block numbers to query event logs in order
	for _, hash := range originFilter.BlockHashes {
		block, err := cfx.GetBlockSummaryByHash(hash)
		if err != nil {
			return nil, false, err
		}

		// Fullnode will return error if any block hash not found.
		// Error processing request: Filter error: Unable to identify block 0xaaaa...
		if block == nil {
			return nil, false, fmt.Errorf("unable to identify block %v", hash)
		}

		bn := int(block.BlockNumber.ToInt().Uint64())

		if _, ok := blockNumber2HashCache[bn]; !ok {
			blockNumber2HashCache[bn] = hash
			blockNumbers = append(blockNumbers, bn)
		}
	}

	// Blocks not found
	if len(blockNumbers) == 0 {
		return nil, true, nil
	}

	// To query event logs in order
	sort.Ints(blockNumbers)

	maxLogs := store.MaxLogLimit
	if originFilter.Offset != nil {
		maxLogs += uint64(*originFilter.Offset)
	}

	var result []types.Log
	start := time.Now()

	// Query in DESC
	for i := len(blockNumbers) - 1; i >= 0; i-- {
		blockHash := blockNumber2HashCache[blockNumbers[i]]
		filter := types.LogFilter{
			BlockHashes: []types.Hash{blockHash},
			Address:     originFilter.Address,
			Topics:      originFilter.Topics,
			// ignore offset and limit
		}

		// Query event logs for a single block
		logs, hit, err := h.getLogsBySingleBlockHash(ctx, cfx, &filter)
		hitStore = hitStore || hit
		if err != nil {
			return nil, hitStore, err
		}

		// Check timeout in each iteration
		if time.Since(start) > store.TimeoutGetLogs {
			return nil, hitStore, store.ErrGetLogsTimeout
		}

		result = append(logs, result...)

		// Check returned number of event logs
		if uint64(len(result)) > maxLogs {
			return nil, hitStore, store.ErrGetLogsResultSetTooLarge
		}
	}

	// Offset: skip the last N logs
	if originFilter.Offset != nil && *originFilter.Offset > 0 {
		offset := int(*originFilter.Offset)
		total := len(result)

		if offset >= total {
			return nil, hitStore, nil
		}

		result = result[0 : total-offset]
	}

	// Limit: return the last N logs
	if originFilter.Limit != nil && *originFilter.Limit > 0 {
		limit := int(*originFilter.Limit)
		total := len(result)

		if limit >= total {
			return result, hitStore, nil
		}

		result = result[total-limit:]
	}

	return result, hitStore, nil
}

func (h *CfxLogsApiHandler) getLogsBySingleBlockHash(
	ctx context.Context, cfx sdk.ClientOperator, filter *types.LogFilter) ([]types.Log, bool, error) {
	sfilter, ok := store.ParseLogFilter(cfx, filter)
	if !ok {
		return nil, false, store.ErrUnsupported
	}

	logs, err := h.storeHandler.GetLogs(ctx, *sfilter)
	if err == nil { // hit right in the store
		return logs, true, nil
	}

	if errors.Is(err, store.ErrAlreadyPruned) { // event logs already pruned?
		logrus.WithField("filter", filter).Debug(
			"Event log data already pruned for `cfx_getLogs` within store",
		)

		// try to get logs from pruned logs handler if could
		if h.prunedHandler != nil {
			logs, err := h.prunedHandler.GetLogs(ctx, *filter)
			return logs, false, err
		}

		return nil, false, errEventLogsTooStale
	}

	// skip fullnode delegation as these errors might indicate heavy workloads or
	// meaningless operation on fullnode.
	for interceptErr, hit := range h.interceptableErrMap {
		if errors.Is(err, interceptErr) {
			return nil, hit, err
		}
	}

	logrus.WithField("fullnode", cfx.GetNodeURL()).WithError(err).Debug(
		"Delegate `cfx_getLogs` to fullnode due to api handler failed to handle",
	)

	// for any error, delegate request to full node, including:
	// 1. database level error
	// 2. record not found (log range mismatch)
	// TODO: add rate limit in case of fullnode being crushed by heavy workloads.
	logs, err = cfx.GetLogs(*filter)
	return logs, false, err
}

func (h *CfxLogsApiHandler) getLogsByRange(ctx context.Context, cfx sdk.ClientOperator, filter *types.LogFilter) ([]types.Log, bool, error) {
	start := time.Now()

	// record the reorg version before query to ensure data consistence
	lastReorgVersion, err := h.ms.GetReorgVersion()
	if err != nil {
		return nil, false, err
	}

	for {
		logs, hitStore, err := h.getLogsReorgGuard(ctx, cfx, filter)
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
		}).Debug("Reorg detected, retry query corespace event logs")

		// when reorg occurred, check timeout before retry.
		if time.Since(start) > store.TimeoutGetLogs {
			return nil, false, store.ErrGetLogsTimeout
		}

		// reorg version changed during data query and try again.
		lastReorgVersion = reorgVersion
	}
}

func (h *CfxLogsApiHandler) getLogsReorgGuard(ctx context.Context, cfx sdk.ClientOperator, filter *types.LogFilter) ([]types.Log, bool, error) {
	// try to query event logs from database and fullnode
	dbFilter, fnFilter, err := h.splitLogFilter(cfx, filter)
	if err != nil {
		return nil, false, err
	}

	var logs []types.Log

	// query data from database
	if dbFilter != nil {
		sfilter, ok := store.ParseLogFilter(cfx, filter)
		if !ok {
			return nil, false, store.ErrUnsupported
		}

		dbLogs, err := h.storeHandler.GetLogs(ctx, *sfilter)
		if err != nil {
			return nil, false, err
		}

		logrus.WithField("filter", dbFilter).Debug("Loaded corespace event logs from store")

		logs = append(logs, dbLogs...)
	}

	// query data from fullnode
	if fnFilter != nil {
		fnLogs, err := cfx.GetLogs(*fnFilter)
		if err != nil {
			return nil, false, err
		}

		logrus.WithField("filter", fnFilter).Debug("Loaded corespace event logs from fullnode")

		logs = append(logs, fnLogs...)
	}

	// will remove Limit filter in future, and do not care about perf now.
	if filter.Limit != nil && len(logs) > int(*filter.Limit) {
		logs = logs[len(logs)-int(*filter.Limit):]
	}

	return logs, dbFilter != nil, nil
}

// splitLogFilter splits log filter into 2 parts to query data from database or fullnode.
func (h *CfxLogsApiHandler) splitLogFilter(cfx sdk.ClientOperator, filter *types.LogFilter) (*types.LogFilter, *types.LogFilter, error) {
	minEpoch, maxEpoch, err := h.ms.GetGlobalEpochRange()
	if err != nil {
		return nil, nil, err
	}

	sfilter, ok := store.ParseLogFilter(cfx, filter)
	if !ok {
		return nil, nil, store.ErrUnsupported
	}

	var epochFrom, epochTo uint64

	switch {
	case sfilter.EpochRange != nil:
		epochFrom = sfilter.EpochRange.From
		epochTo = sfilter.EpochRange.To
	case sfilter.BlockRange != nil:
		if epochFrom, err = h.blockNumber2EpochNumber(cfx, sfilter.BlockRange.From); err != nil {
			return nil, nil, err
		}

		if epochTo, err = h.blockNumber2EpochNumber(cfx, sfilter.BlockRange.To); err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, store.ErrUnsupported
	}

	logger := logrus.WithFields(logrus.Fields{
		"sfilter": sfilter, "minEpoch": minEpoch, "maxEpoch": maxEpoch,
	})

	// some data pruned
	if epochFrom < minEpoch {
		logger.Debug("Some corespace event logs already pruned")
		return nil, nil, store.ErrAlreadyPruned
	}

	// all data in database
	if epochTo <= maxEpoch {
		logger.Debug("All coreSpace event logs in database")
		return filter, nil, nil
	}

	metrics.GetOrRegisterHistogram(nil, "rpc/cfx_getLogs/split/fn").Update(int64(epochTo - maxEpoch))

	// no data in database
	if epochFrom > maxEpoch {
		logger.Debug("All coreSpace event logs are not in database")
		return nil, filter, nil
	}

	// otherwise, partial data in databse
	dbFilter, fnFilter := *filter, *filter

	switch {
	case sfilter.EpochRange != nil:
		dbFilter.ToEpoch = types.NewEpochNumberUint64(maxEpoch)
		fnFilter.FromEpoch = types.NewEpochNumberUint64(maxEpoch + 1)
	case sfilter.BlockRange != nil:
		block, err := cfx.GetBlockSummaryByEpoch(types.NewEpochNumberUint64(maxEpoch))
		if err != nil {
			return nil, nil, err
		}

		if block == nil {
			return nil, filter, nil
		}

		dbFilter.ToBlock = block.BlockNumber
		fnFilter.FromBlock = (*hexutil.Big)(big.NewInt(block.BlockNumber.ToInt().Int64() + 1))
	default:
		return nil, nil, store.ErrUnsupported
	}

	logger.WithFields(logrus.Fields{
		"dbFilter": dbFilter, "fnFilter": fnFilter,
	}).Debug("Corespace event logs are partially in database")

	return &dbFilter, &fnFilter, nil
}

func (h *CfxLogsApiHandler) blockNumber2EpochNumber(cfx sdk.ClientOperator, bn uint64) (uint64, error) {
	block, err := cfx.GetBlockSummaryByBlockNumber(hexutil.Uint64(bn))
	if err != nil {
		return 0, err
	}

	if block == nil {
		return 0, fmt.Errorf("unable to find block hash for block %v", bn)
	}

	return block.EpochNumber.ToInt().Uint64(), nil
}
