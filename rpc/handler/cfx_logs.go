package handler

import (
	"context"
	"fmt"
	"math/big"
	"sort"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/metrics"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
)

var (
	errEventLogsTooStale = errors.New("event logs are too stale (already pruned)")
)

// CfxLogsApiHandler RPC handler to get core space event logs from store or fullnode.
type CfxLogsApiHandler struct {
	ms *mysql.MysqlStore

	prunedHandler *CfxPrunedLogsHandler // optional
}

func NewCfxLogsApiHandler(ms *mysql.MysqlStore, prunedHandler *CfxPrunedLogsHandler) *CfxLogsApiHandler {
	return &CfxLogsApiHandler{ms, prunedHandler}
}

func (handler *CfxLogsApiHandler) GetLogs(
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
		if err := checkTimeout(timeoutCtx); err != nil {
			return nil, false, err
		}

		// reorg version changed during data query and try again.
		lastReorgVersion = reorgVersion
	}
}

func (handler *CfxLogsApiHandler) getLogsReorgGuard(
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

	metrics.Registry.RPC.Percentage("cfx_getLogs", "filter/split/alldatabase").Mark(fnFilter == nil)
	metrics.Registry.RPC.Percentage("cfx_getLogs", "filter/split/allfullnode").Mark(len(dbFilters) == 0)
	metrics.Registry.RPC.Percentage("cfx_getLogs", "filter/split/partial").Mark(len(dbFilters) > 0 && fnFilter != nil)

	var logs []types.Log

	// query data from database
	for i := range dbFilters {
		if err := checkTimeout(ctx); err != nil {
			return nil, false, err
		}

		dbLogs, err := handler.ms.GetLogs(ctx, dbFilters[i])

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

		// try to query pruned logs from archive fullnode
		originalFilter := dbFilters[i].Cfx()
		if originalFilter == nil {
			return nil, false, errors.WithMessage(
				errEventLogsTooStale, "missing original log filter",
			)
		}

		// ensure fullnode delegation is rational
		if err := handler.checkFullnodeLogFilter(originalFilter); err != nil {
			return nil, false, err
		}

		fnLogs, err := handler.prunedHandler.GetLogs(ctx, *originalFilter)
		if err != nil {
			return nil, false, err
		}

		logs = append(logs, fnLogs...)
	}

	// query data from fullnode
	if fnFilter != nil {
		// timeout check before fullnode delegation
		if err := checkTimeout(ctx); err != nil {
			return nil, false, err
		}

		// ensure split log filter for fullnode is rational
		if err := handler.checkFullnodeLogFilter(fnFilter); err != nil {
			return nil, false, err
		}

		fnLogs, err := cfx.GetLogs(*fnFilter)
		if err != nil {
			return nil, false, err
		}

		logs = append(logs, fnLogs...)
	}

	// ensure result set never oversized
	if len(logs) > int(store.MaxLogLimit) {
		return nil, false, store.ErrGetLogsResultSetTooLarge
	}

	return logs, len(dbFilters) > 0, nil
}

func (handler *CfxLogsApiHandler) splitLogFilter(
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
) ([]store.LogFilter, *types.LogFilter, error) {
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

func (handler *CfxLogsApiHandler) splitLogFilterByBlockHashes(
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
	maxEpoch uint64,
) ([]store.LogFilter, *types.LogFilter, error) {
	var dbBlockNumbers []int
	var fnBlockHashes []types.Hash

	blockNumToHash := make(map[int]types.Hash)

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
		if _, ok := blockNumToHash[bn]; ok {
			continue
		}

		blockNumToHash[bn] = hash

		if epoch := block.EpochNumber.ToInt().Uint64(); epoch <= maxEpoch {
			dbBlockNumbers = append(dbBlockNumbers, bn)
		} else {
			fnBlockHashes = append(fnBlockHashes, hash)
		}
	}

	sort.Ints(dbBlockNumbers) // sort block numbers ascendingly

	var dbFilters []store.LogFilter
	for _, bn := range dbBlockNumbers {
		partialFilter := *filter
		partialFilter.BlockHashes = []types.Hash{blockNumToHash[bn]}

		logfilter := store.ParseCfxLogFilter(uint64(bn), uint64(bn), &partialFilter)
		dbFilters = append(dbFilters, logfilter)
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

func (handler *CfxLogsApiHandler) splitLogFilterByBlockRange(
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
	maxBlock uint64,
) ([]store.LogFilter, *types.LogFilter, error) {
	// no data in database
	blockFrom := filter.FromBlock.ToInt().Uint64()
	if blockFrom > maxBlock {
		return nil, filter, nil
	}

	// all data in database
	blockTo := filter.ToBlock.ToInt().Uint64()
	if blockTo <= maxBlock {
		dbFilter := store.ParseCfxLogFilter(blockFrom, blockTo, filter)
		return []store.LogFilter{dbFilter}, nil, nil
	}

	// otherwise, partial data in databse
	partialFilter := *filter
	partialFilter.ToBlock = (*hexutil.Big)(big.NewInt(int64(maxBlock)))
	dbFilter := store.ParseCfxLogFilter(blockFrom, maxBlock, &partialFilter)

	fnFilter := types.LogFilter{
		FromBlock: types.NewBigInt(maxBlock + 1),
		ToBlock:   filter.ToBlock,
		Address:   filter.Address,
		Topics:    filter.Topics,
	}

	return []store.LogFilter{dbFilter}, &fnFilter, nil
}

func (handler *CfxLogsApiHandler) splitLogFilterByEpochRange(
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
	maxEpoch, maxBlock uint64,
) ([]store.LogFilter, *types.LogFilter, error) {
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
		return []store.LogFilter{dbFilter}, nil, nil
	}

	// otherwise, partial data in databse
	partialFilter := *filter
	partialFilter.ToEpoch = types.NewEpochNumberUint64(maxEpoch)
	dbFilter := store.ParseCfxLogFilter(blockFrom, maxBlock, &partialFilter)

	fnFilter := types.LogFilter{
		FromEpoch: types.NewEpochNumberUint64(maxEpoch + 1),
		ToEpoch:   filter.ToEpoch,
		Address:   filter.Address,
		Topics:    filter.Topics,
	}

	return []store.LogFilter{dbFilter}, &fnFilter, nil
}

// checkFullnodeLogFilter checks if the log filter is rational for fullnode delegation.
//
// Note this function assumes the log filter is valid and normalized.
func (handler *CfxLogsApiHandler) checkFullnodeLogFilter(filter *types.LogFilter) error {
	// epoch range bound checking
	if filter.FromEpoch != nil && filter.ToEpoch != nil {
		ef, _ := filter.FromEpoch.ToInt()
		et, _ := filter.ToEpoch.ToInt()
		epochFrom, epochTo := ef.Uint64(), et.Uint64()

		if epochTo-epochFrom+1 > store.MaxLogEpochRange {
			return store.ErrGetLogsQuerySetTooLarge
		}
	}

	// block range bound checking
	if filter.FromBlock != nil && filter.ToBlock != nil {
		fromBlock := filter.FromBlock.ToInt().Uint64()
		toBlock := filter.ToBlock.ToInt().Uint64()
		if toBlock-fromBlock+1 > store.MaxLogBlockRange {
			return store.ErrGetLogsQuerySetTooLarge
		}
	}

	return nil
}

// checkTimeout checks if operation is timed out.
func checkTimeout(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return store.ErrGetLogsTimeout
	default:
	}

	return nil
}
