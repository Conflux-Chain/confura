package handler

import (
	"context"
	"fmt"
	"math/big"
	"sort"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util/metrics"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	// Maximum number of bytes for the response body of getLogs requests
	maxGetLogsResponseBytes     uint64
	errResponseBodySizeTooLarge error

	errEventLogsTooStale = errors.New("event logs are too stale (already pruned)")
)

func MustInitFromViper() {
	var resrcLimit struct {
		MaxGetLogsResponseBytes uint64 `default:"10485760"` // default 10MB
	}
	viperutil.MustUnmarshalKey("requestControl.resourceLimits", &resrcLimit)

	maxGetLogsResponseBytes = resrcLimit.MaxGetLogsResponseBytes
	errResponseBodySizeTooLarge = fmt.Errorf(
		"result body size is too large with more than %d bytes, please narrow down your filter condition",
		maxGetLogsResponseBytes,
	)
}

// CfxLogsApiHandler RPC handler to get core space event logs from store or fullnode.
type CfxLogsApiHandler struct {
	ms *mysql.MysqlStore

	prunedHandler      *CfxPrunedLogsHandler // optional
	maxSuggestAttempts int
}

func NewCfxLogsApiHandler(ms *mysql.MysqlStore, prunedHandler *CfxPrunedLogsHandler, maxAttempts int) *CfxLogsApiHandler {
	return &CfxLogsApiHandler{ms: ms, prunedHandler: prunedHandler, maxSuggestAttempts: maxAttempts}
}

func (handler *CfxLogsApiHandler) GetLogs(
	ctx context.Context,
	cfx sdk.ClientOperator,
	filter *types.LogFilter,
	delegatedRpcMethod string,
) ([]types.Log, bool, error) {
	// record the reorg version before query to ensure data consistence
	lastReorgVersion, err := handler.ms.GetReorgVersion()
	if err != nil {
		return nil, false, err
	}

	var (
		suggestErr   error
		suggestCount int
	)

	for {
		logs, hitStore, err := handler.getLogsReorgGuard(ctx, cfx, filter, delegatedRpcMethod)
		if err != nil {
			if maxAttempts := handler.maxSuggestAttempts; maxAttempts > 0 && suggestCount < maxAttempts {
				var (
					suggestBlockRangeErr *store.SuggestedFilterOversizedError[store.SuggestedBlockRange]
					suggestEpochRangeErr *store.SuggestedFilterOversizedError[store.SuggestedEpochRange]
				)
				if filter.ToBlock != nil && errors.As(err, &suggestBlockRangeErr) {
					suggestErr = suggestBlockRangeErr
					filter.ToBlock = (*hexutil.Big)(big.NewInt(0).SetUint64(suggestBlockRangeErr.SuggestedRange.To))
				} else if filter.ToEpoch != nil && errors.As(err, &suggestEpochRangeErr) {
					suggestErr = suggestEpochRangeErr
					filter.ToEpoch = types.NewEpochNumberUint64(suggestEpochRangeErr.SuggestedRange.To)
				}

				if suggestBlockRangeErr != nil || suggestEpochRangeErr != nil {
					// check timeout before retry.
					if err := checkTimeout(ctx); err != nil {
						return nil, false, err
					}

					suggestCount++
					continue
				}
			}

			if suggestErr == nil {
				return nil, false, err
			}
		}

		// If for any reason a suggestion error was set, propagate it.
		if suggestErr != nil {
			return nil, false, suggestErr
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
		if err := checkTimeout(ctx); err != nil {
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
	delegatedRpcMethod string,
) ([]types.Log, bool, error) {
	// Try to query event logs from database and fullnode.
	// Note, if multiple block hashes specified in log filter, then split the block hashes
	// into multiple block number ranges.
	dbFilters, fnFilter, err := handler.splitLogFilter(cfx, filter)
	if err != nil {
		return nil, false, err
	}

	if len(delegatedRpcMethod) > 0 {
		metrics.Registry.RPC.Percentage(delegatedRpcMethod, "filter/split/alldatabase").Mark(fnFilter == nil)
		metrics.Registry.RPC.Percentage(delegatedRpcMethod, "filter/split/allfullnode").Mark(len(dbFilters) == 0)
		metrics.Registry.RPC.Percentage(delegatedRpcMethod, "filter/split/partial").Mark(len(dbFilters) > 0 && fnFilter != nil)

		if blkRange, valid := calculateCfxBlockRange(fnFilter); valid {
			numBlocks := int64(blkRange.To - blkRange.From + 1)
			metrics.Registry.RPC.LogFilterSplit(delegatedRpcMethod, "fullnode/blockRange").Update(numBlocks)
		} else if epochRange, valid := calculateEpochRange(fnFilter); valid {
			numEpochs := int64(epochRange.To - epochRange.From + 1)
			metrics.Registry.RPC.LogFilterSplit(delegatedRpcMethod, "fullnode/epochRange").Update(numEpochs)
		}
	}

	var logs []types.Log
	var accumulator int

	useBoundCheck := handler.RequireBoundChecks(filter)
	if len(dbFilters) > 0 {
		if useBoundCheck {
			// add db query timeout
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, store.TimeoutGetLogs)
			defer cancel()
		} else {
			ctx = store.NewContextWithBoundChecksDisabled(ctx)
		}
	}

	// query data from database
	for i := range dbFilters {
		if err := checkTimeout(ctx); err != nil {
			return nil, false, err
		}

		dbLogs, err := handler.ms.GetLogs(ctx, dbFilters[i])

		// succeeded to get logs from database
		if err == nil {
			for _, v := range dbLogs {
				if accumulator += len(v.Extra); useBoundCheck && uint64(accumulator) > maxGetLogsResponseBytes {
					return nil, false, newSuggestedBodyBytesOversizedError(cfx, filter, v)
				}

				log, _ := v.ToCfxLog()
				logs = append(logs, *log)
			}

			continue
		}

		// convert suggested block range back to epoch range for log filter with epoch range
		if filter.FromEpoch != nil {
			var valErr *store.SuggestedFilterOversizedError[store.SuggestedBlockRange]
			if errors.As(err, &valErr) {
				oversizedErr := handler.convertSuggestedFilterOversizedError(filter, valErr)
				return nil, false, oversizedErr
			}
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

		for i := range fnLogs {
			if accumulator += len(fnLogs[i].Data); useBoundCheck && uint64(accumulator) > maxGetLogsResponseBytes {
				return nil, false, newSuggestedBodyBytesOversizedError(cfx, filter, &fnLogs[i])
			}
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

		for i := range fnLogs {
			if accumulator += len(fnLogs[i].Data); useBoundCheck && uint64(accumulator) > maxGetLogsResponseBytes {
				return nil, false, newSuggestedBodyBytesOversizedError(cfx, filter, &fnLogs[i])
			}
		}
		logs = append(logs, fnLogs...)
	}

	// ensure result set never oversized
	if useBoundCheck && uint64(len(logs)) > store.MaxLogLimit {
		return nil, false, newSuggestedResultSetOversizedError(cfx, filter, &logs[store.MaxLogLimit])
	}

	// Rare case: log context information for diagnostic purposes if the result exceeds limits.
	if uint64(len(logs)) > store.MaxLogLimit || uint64(accumulator) > maxGetLogsResponseBytes {
		logrus.WithFields(logrus.Fields{
			"logFilter":         filter,
			"databaseFilters":   dbFilters,
			"fullnodeFilter":    fnFilter,
			"boundCheckEnabled": useBoundCheck,
			"resultSetCount":    len(logs),
			"responseSizeBytes": uint64(accumulator),
		}).Info("Exceeded limits for getLogs response")
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

	e2bMap, ok, err := handler.ms.BlockMapping(maxEpoch)
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
		return handler.splitLogFilterByBlockRange(cfx, filter, e2bMap.BnMax)
	}

	return handler.splitLogFilterByEpochRange(cfx, filter, maxEpoch, e2bMap.BnMax)
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

		if block.BlockNumber == nil { // block already mined but not ordered yet?
			return nil, nil, fmt.Errorf("block with hash %v is not executed yet", hash)
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
	e2bmap, ok, err := handler.ms.CeilBlockMapping(epochFrom.Uint64())
	if err != nil {
		return nil, nil, err
	}

	if !ok {
		return nil, filter, nil
	}

	blockFrom := e2bmap.BnMin

	// all data in database
	if epochTo.Uint64() <= maxEpoch {
		e2bmap, ok, err = handler.ms.FloorBlockMapping(epochTo.Uint64())
		if err != nil {
			return nil, nil, err
		}

		if !ok {
			return nil, filter, nil
		}

		blockTo := e2bmap.BnMax

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
	// Epoch range bound checking
	if epochRange, valid := calculateEpochRange(filter); valid {
		numEpochs := epochRange.To - epochRange.From + 1
		if numEpochs > uint64(store.MaxLogEpochRange) {
			epochRange.To = epochRange.From + uint64(store.MaxLogEpochRange) - 1
			suggestedRange := store.NewSuggestedEpochRange(epochRange.From, epochRange.To)
			return store.NewSuggestedFilterQuerySetTooLargeError(&suggestedRange)
		}
	}

	// Block range bound checking
	if blockRange, valid := calculateCfxBlockRange(filter); valid {
		numBlocks := blockRange.To - blockRange.From + 1
		if numBlocks > uint64(store.MaxLogBlockRange) {
			blockRange.To = blockRange.From + uint64(store.MaxLogBlockRange) - 1
			suggestedRange := store.SuggestedBlockRange{RangeUint64: blockRange}
			return store.NewSuggestedFilterQuerySetTooLargeError(&suggestedRange)
		}
	}

	return nil
}

// convert suggested block range back to epoch range if possible
func (handler *CfxLogsApiHandler) convertSuggestedFilterOversizedError(
	filter *types.LogFilter, oversizedErr *store.SuggestedFilterOversizedError[store.SuggestedBlockRange]) error {

	if oversizedErr.SuggestedRange.MaxEndEpoch == 0 {
		return oversizedErr.Unwrap()
	}

	fromEpoch, _ := filter.FromEpoch.ToInt()
	maxPossibleEpochNum := oversizedErr.SuggestedRange.MaxEndEpoch
	endBlockNum := oversizedErr.SuggestedRange.To

	suggstedEndEpoch, ok, err := handler.ms.LatestEpochBeforeBlock(maxPossibleEpochNum, endBlockNum)
	if err != nil || !ok || suggstedEndEpoch < fromEpoch.Uint64() {
		return oversizedErr.Unwrap()
	}

	suggestedEpochRange := store.NewSuggestedEpochRange(fromEpoch.Uint64(), suggstedEndEpoch)
	return store.NewSuggestedFilterOversizeError(oversizedErr.Unwrap(), suggestedEpochRange)
}

// RequireBoundChecks determines if bound checks should be applied based on if there is any space to narrow down the log filter
func (handler *CfxLogsApiHandler) RequireBoundChecks(filter *types.LogFilter) bool {
	switch {
	case filter.FromEpoch != nil && filter.ToEpoch != nil:
		return !filter.FromEpoch.Equals(filter.ToEpoch)
	case filter.FromBlock != nil && filter.ToBlock != nil:
		return filter.FromBlock.ToInt().Cmp(filter.ToBlock.ToInt()) < 0
	default:
		return len(filter.BlockHashes) > 1
	}
}

func newSuggestedBodyBytesOversizedError[T types.Log | store.Log](
	cfx sdk.ClientOperator, filter *types.LogFilter, exceedingLog *T) error {
	return newSuggestedFilterOversizedError[T](errResponseBodySizeTooLarge, cfx, filter, exceedingLog)
}

func newSuggestedResultSetOversizedError[T types.Log | store.Log](
	cfx sdk.ClientOperator, filter *types.LogFilter, exceedingLog *T) error {
	return newSuggestedFilterOversizedError[T](store.ErrFilterResultSetTooLarge, cfx, filter, exceedingLog)
}

func newSuggestedFilterOversizedError[T types.Log | store.Log](
	inner error, cfx sdk.ClientOperator, filter *types.LogFilter, exceedingLog *T) error {

	var logEpochNum, logBlockNum uint64
	switch v := any(exceedingLog).(type) {
	case *store.Log:
		if filter.FromEpoch != nil {
			logEpochNum = v.Epoch
		} else if filter.FromBlock != nil {
			logBlockNum = v.BlockNumber
		}
	case *types.Log:
		if filter.FromEpoch != nil {
			logEpochNum = v.EpochNumber.ToInt().Uint64()
		} else if filter.FromBlock != nil && v.BlockHash != nil {
			if block, err := cfx.GetBlockSummaryByHash(*v.BlockHash); err == nil {
				logBlockNum = block.BlockNumber.ToInt().Uint64()
			}
		}
	}

	// Return early if no valid `logEpochNum` or `logBlockNum` is found
	if logEpochNum == 0 && logBlockNum == 0 {
		return inner
	}

	// Suggest filter adjustments based on `FromEpoch` or `FromBlock`
	if filter.FromEpoch != nil {
		fromEpoch, _ := filter.FromEpoch.ToInt()
		if logEpochNum > fromEpoch.Uint64() {
			return store.NewSuggestedFilterOversizeError(
				inner,
				store.NewSuggestedEpochRange(fromEpoch.Uint64(), logEpochNum-1),
			)
		}
	} else if filter.FromBlock != nil {
		fromBlock := filter.FromBlock.ToInt()
		if logBlockNum > fromBlock.Uint64() {
			return store.NewSuggestedFilterOversizeError(
				inner,
				store.NewSuggestedBlockRange(fromBlock.Uint64(), logBlockNum-1, 0),
			)
		}
	}

	return inner
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

// calculateCfxBlockRange calculates the block range from the log filter.
func calculateCfxBlockRange(filter *types.LogFilter) (blockRange citypes.RangeUint64, ok bool) {
	if filter == nil || filter.FromBlock == nil || filter.ToBlock == nil {
		return blockRange, false
	}

	bf := filter.FromBlock.ToInt()
	bt := filter.ToBlock.ToInt()
	if bf.Uint64() > bt.Uint64() {
		return blockRange, false
	}

	blockRange.From, blockRange.To = bf.Uint64(), bt.Uint64()
	return blockRange, true
}

// calculateEpochRange calculates the epoch range from the log filter.
func calculateEpochRange(filter *types.LogFilter) (epochRange citypes.RangeUint64, ok bool) {
	if filter == nil || filter.FromEpoch == nil || filter.ToEpoch == nil {
		return epochRange, false
	}

	ef, _ := filter.FromEpoch.ToInt()
	et, _ := filter.ToEpoch.ToInt()
	if ef.Uint64() > et.Uint64() {
		return epochRange, false
	}

	epochRange.From, epochRange.To = ef.Uint64(), et.Uint64()
	return epochRange, true
}
