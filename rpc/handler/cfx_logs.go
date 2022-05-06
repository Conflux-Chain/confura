package handler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
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
}

func NewCfxLogsApiHandler(sh CfxStoreHandler, ph *CfxPrunedLogsHandler) *CfxLogsApiHandler {
	return &CfxLogsApiHandler{
		storeHandler: sh, prunedHandler: ph,
		interceptableErrMap: map[error]bool{
			store.ErrGetLogsResultSetTooLarge: true, store.ErrGetLogsTimeout: false,
		},
	}
}

func (h *CfxLogsApiHandler) GetLogs(
	ctx context.Context, cfx sdk.ClientOperator, filter *types.LogFilter) ([]types.Log, bool, error) {
	if len(filter.BlockHashes) > 1 {
		return h.getLogsByBlockHashes(ctx, cfx, filter)
	}

	return h.getLogs(ctx, cfx, filter)
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
		logs, hit, err := h.getLogs(ctx, cfx, &filter)
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

func (h *CfxLogsApiHandler) getLogs(
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
