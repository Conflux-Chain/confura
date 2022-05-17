package handler

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/store/mysql"
)

type CfxLogsApiHandlerV2 struct {
	ms *mysql.MysqlStore

	prunedHandler *CfxPrunedLogsHandler // optional
}

func NewCfxLogsApiHandlerV2(ms *mysql.MysqlStore, prunedHandler *CfxPrunedLogsHandler) *CfxLogsApiHandlerV2 {
	return &CfxLogsApiHandlerV2{ms, prunedHandler}
}

func (handler *CfxLogsApiHandlerV2) GetLogs(
	ctx context.Context,
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
) ([]types.Log, bool, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, store.TimeoutGetLogs)
	defer cancel()

	// record the reorg version before query to ensure data consistence
	lastReorgVersion, err := handler.ms.GetReorgVersion()
	if err != nil {
		return nil, false, err
	}

	for {
		logs, hitStore, err := handler.getLogsReorgGuard(timeoutCtx, cfx, filter)
		if err != nil {
			return nil, false, err
		}

		// check the reorg version after query
		reorgVersion, err := handler.ms.GetReorgVersion()
		if err != nil {
			return nil, false, err
		}

		if reorgVersion == lastReorgVersion {
			return logs, hitStore, nil
		}

		// when reorg occurred, check timeout before retry.
		select {
		case <-timeoutCtx.Done():
			return nil, false, store.ErrGetLogsTimeout
		default:
		}

		// reorg version changed during data query and try again.
		lastReorgVersion = reorgVersion
	}
}

func (handler *CfxLogsApiHandlerV2) getLogsReorgGuard(
	ctx context.Context,
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
) ([]types.Log, bool, error) {
	// Try to query event logs from database and fullnode.
	// Note, if multiple block hashes specified in log filter, then split the block hashes
	// into multiple block number ranges.
	dbFilters, fnFilter, err := handler.splitLogFilter(cfx, filter)
	if err != nil {
		return nil, false, err
	}

	var logs []types.Log

	// query data from database
	for i := range dbFilters {
		dbLogs, err := handler.ms.GetLogsV2(ctx, dbFilters[i])

		// succeeded to get logs from database
		if err == nil {
			for _, v := range dbLogs {
				log, _ := v.ToCfxLog()
				logs = append(logs, *log)
			}

			continue
		}

		if !errors.Is(err, store.ErrAlreadyPruned) {
			return nil, false, err
		}

		// data already pruned
		if handler.prunedHandler == nil {
			return nil, false, errEventLogsTooStale
		}

		fnLogs, err := handler.prunedHandler.GetLogs(ctx, *filter)
		if err != nil {
			return nil, false, err
		}

		logs = append(logs, fnLogs...)
	}

	// query data from fullnode
	if fnFilter != nil {
		fnLogs, err := cfx.GetLogs(*fnFilter)
		if err != nil {
			return nil, false, err
		}

		logs = append(logs, fnLogs...)
	}

	return logs, len(dbFilters) > 0, nil
}

func (handler *CfxLogsApiHandlerV2) splitLogFilter(
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
) ([]store.LogFilterV2, *types.LogFilter, error) {
	maxEpoch, ok, err := handler.ms.MaxEpoch()
	if err != nil {
		return nil, nil, err
	}

	if !ok {
		return nil, filter, nil
	}

	blockRange, ok, err := handler.ms.BlockRange(maxEpoch)
	if err != nil {
		return nil, nil, err
	}

	if !ok {
		return nil, filter, nil
	}

	if len(filter.BlockHashes) > 0 {
		return handler.splitLogFilterByBlockHashes(cfx, filter, maxEpoch)
	}

	if filter.FromBlock != nil && filter.ToBlock != nil {
		return handler.splitLogFilterByBlockRange(cfx, filter, blockRange.To)
	}

	return handler.splitLogFilterByEpochRange(cfx, filter, maxEpoch, blockRange.To)
}

func (handler *CfxLogsApiHandlerV2) splitLogFilterByBlockHashes(
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
	maxEpoch uint64,
) ([]store.LogFilterV2, *types.LogFilter, error) {
	var dbBlockNumbers []int
	cache := make(map[int]bool)
	var fnBlockHashes []types.Hash

	// convert block hash to number to query from database
	for _, hash := range filter.BlockHashes {
		block, err := cfx.GetBlockSummaryByHash(hash)
		if err != nil {
			return nil, nil, err
		}

		// Fullnode will return error if any block hash not found.
		// Error processing request: Filter error: Unable to identify block 0xaaaa...
		if block == nil {
			return nil, nil, fmt.Errorf("unable to identify block %v", hash)
		}

		bn := int(block.BlockNumber.ToInt().Uint64())

		// dedupe
		if cache[bn] {
			continue
		}

		if epoch := block.EpochNumber.ToInt().Uint64(); epoch <= maxEpoch {
			dbBlockNumbers = append(dbBlockNumbers, bn)
		} else {
			fnBlockHashes = append(fnBlockHashes, hash)
		}
	}

	var dbFilters []store.LogFilterV2
	for _, bn := range dbBlockNumbers {
		dbFilters = append(dbFilters, store.ParseCfxLogFilter(uint64(bn), uint64(bn), filter))
	}

	if len(fnBlockHashes) == 0 {
		return dbFilters, nil, nil
	}

	return dbFilters, &types.LogFilter{
		BlockHashes: fnBlockHashes,
		Address:     filter.Address,
		Topics:      filter.Topics,
	}, nil
}

func (handler *CfxLogsApiHandlerV2) splitLogFilterByBlockRange(
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
	maxBlock uint64,
) ([]store.LogFilterV2, *types.LogFilter, error) {
	// no data in database
	blockFrom := filter.FromBlock.ToInt().Uint64()
	if blockFrom > maxBlock {
		return nil, filter, nil
	}

	// all data in database
	blockTo := filter.ToBlock.ToInt().Uint64()
	if blockTo <= maxBlock {
		dbFilter := store.ParseCfxLogFilter(blockFrom, blockTo, filter)
		return []store.LogFilterV2{dbFilter}, nil, nil
	}

	// otherwise, partial data in databse
	dbFilter := store.ParseCfxLogFilter(blockFrom, maxBlock, filter)
	fnFilter := types.LogFilter{
		FromBlock: types.NewBigInt(maxBlock + 1),
		ToBlock:   filter.ToBlock,
		Address:   filter.Address,
		Topics:    filter.Topics,
	}

	return []store.LogFilterV2{dbFilter}, &fnFilter, nil
}

func (handler *CfxLogsApiHandlerV2) splitLogFilterByEpochRange(
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
	maxEpoch, maxBlock uint64,
) ([]store.LogFilterV2, *types.LogFilter, error) {
	epochFrom, ok := filter.FromEpoch.ToInt()
	if !ok {
		return nil, filter, nil
	}

	// no data in database
	if epochFrom.Uint64() > maxEpoch {
		return nil, filter, nil
	}

	epochTo, ok := filter.ToEpoch.ToInt()
	if !ok {
		return nil, filter, nil
	}

	// convert epoch number to block number
	blockRange, ok, err := handler.ms.BlockRange(epochFrom.Uint64())
	if err != nil {
		return nil, nil, err
	}

	if !ok {
		return nil, filter, nil
	}

	blockFrom := blockRange.From

	// all data in database
	if epochTo.Uint64() <= maxEpoch {
		blockRange, ok, err = handler.ms.BlockRange(epochTo.Uint64())
		if err != nil {
			return nil, nil, err
		}

		if !ok {
			return nil, filter, nil
		}

		blockTo := blockRange.To

		dbFilter := store.ParseCfxLogFilter(blockFrom, blockTo, filter)
		return []store.LogFilterV2{dbFilter}, nil, nil
	}

	// otherwise, partial data in databse
	dbFilter := store.ParseCfxLogFilter(blockFrom, maxBlock, filter)
	fnFilter := types.LogFilter{
		FromEpoch: types.NewEpochNumberUint64(maxEpoch + 1),
		ToEpoch:   filter.ToEpoch,
		Address:   filter.Address,
		Topics:    filter.Topics,
	}

	return []store.LogFilterV2{dbFilter}, &fnFilter, nil
}
