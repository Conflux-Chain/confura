package handler

import (
	"context"
	"strings"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util/metrics"
	metricutil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// EthLogsApiHandler RPC handler to get evm space event logs from store or fullnode.
type EthLogsApiHandler struct {
	es *mysql.EthStore

	maxSuggestAttempts int
}

type ethLogsClient interface {
	ethScanClient
	ethBlockByHashClient
}

type ethBlockByHashClient interface {
	BlockByHash(blockHash common.Hash, isFull bool) (*types.Block, error)
}

func NewEthLogsApiHandler(es *mysql.EthStore, maxAttempts int) *EthLogsApiHandler {
	return &EthLogsApiHandler{es: es, maxSuggestAttempts: maxAttempts}
}

func (handler *EthLogsApiHandler) GetLogs(
	ctx context.Context,
	eth ethLogsClient,
	filter *types.FilterQuery,
	delegatedRpcMethod string,
) ([]types.Log, bool, error) {
	retry := newLogsQueryRetry(ctx, handler.RequiresBoundChecks(filter), "eth_getLogs")
	defer retry.close()
	if err := retry.check(); err != nil {
		return nil, false, err
	}

	// record the reorg version before query to ensure data consistence
	lastReorgVersion, err := handler.es.GetReorgVersion()
	if err != nil {
		return nil, false, err
	}

	var (
		suggestErr   *store.SuggestedFilterOversizedError[store.SuggestedBlockRange]
		suggestCount int
	)

	for {
		if err := retry.check(); err != nil {
			return nil, false, err
		}
		ctx = retry.ctx
		logs, hitStore, err := handler.getLogsWithReorgGuard(ctx, ethLogsWithContext(ctx, eth), filter, delegatedRpcMethod, retry.bounded)
		if timeoutErr := retry.check(); timeoutErr != nil {
			return nil, false, timeoutErr
		}
		if errors.Is(err, ErrGetLogsConsistency) {
			version, versionErr := handler.es.GetReorgVersion()
			if versionErr != nil {
				return nil, false, versionErr
			}
			reason := "stitch"
			var conflict logsConsistencyError
			if errors.As(err, &conflict) {
				reason = string(conflict)
			}
			if version != lastReorgVersion {
				reason = "database"
			}
			lastReorgVersion = version
			if err := retry.retry(reason); err != nil {
				return nil, false, err
			}
			continue
		}
		if err != nil {
			if maxAttempts := handler.maxSuggestAttempts; maxAttempts > 0 && suggestCount < maxAttempts &&
				filter.ToBlock != nil && errors.As(err, &suggestErr) {
				*filter.ToBlock = types.BlockNumber(suggestErr.SuggestedRange.To)

				// check timeout before retry.
				if err := checkTimeout(ctx); err != nil {
					return nil, false, err
				}

				suggestCount++
				continue
			}

			return nil, false, err
		}

		// If for any reason a suggestion error was set, propagate it.
		if suggestErr != nil {
			return nil, false, suggestErr
		}

		// check the reorg version after query
		reorgVersion, err := handler.es.GetReorgVersion()
		if err != nil {
			return nil, false, err
		}

		if reorgVersion == lastReorgVersion {
			return logs, hitStore, nil
		}

		// Discard the entire attempt, including empty results, on any DB reorg.
		lastReorgVersion = reorgVersion
		if err := retry.retry("database"); err != nil {
			return nil, false, err
		}
	}
}

// getLogsWithReorgGuard queries logs under the DB reorg version checks in GetLogs.
// The caller owns version validation and retries; each call builds fresh chain checks.
func (handler *EthLogsApiHandler) getLogsWithReorgGuard(
	ctx context.Context,
	eth ethLogsClient,
	filter *types.FilterQuery,
	delegatedRpcMethod string,
	useBoundCheck bool,
) ([]types.Log, bool, error) {
	// Try to query event logs from database and fullnode.
	dbFilter, fnFilter, err := handler.splitLogFilter(eth, filter)
	if err != nil {
		return nil, false, err
	}

	if len(delegatedRpcMethod) > 0 {
		metrics.Registry.RPC.Percentage(delegatedRpcMethod, "filter/split/alldatabase").Mark(fnFilter == nil)
		metrics.Registry.RPC.Percentage(delegatedRpcMethod, "filter/split/allfullnode").Mark(dbFilter == nil)
		metrics.Registry.RPC.Percentage(delegatedRpcMethod, "filter/split/partial").Mark(dbFilter != nil && fnFilter != nil)

		if blockRange, valid := calculateEthBlockRange(fnFilter); valid {
			numBlocks := blockRange.To - blockRange.From + 1
			metrics.Registry.RPC.LogFilterSplit(delegatedRpcMethod, "fullnode/blockRange").Update(int64(numBlocks))
		}
	}

	var logs []types.Log
	var responseSize logArraySize

	if dbFilter != nil {
		if !useBoundCheck {
			ctx = store.NewContextWithBoundChecksDisabled(ctx)
		}

		// query data from database
		dbLogs, err := handler.es.GetLogs(ctx, *dbFilter)
		if err != nil {
			// TODO ErrPrunedAlready
			return nil, false, err
		}

		for _, v := range dbLogs {
			log, err := v.ToEthLog()
			if err != nil {
				return nil, false, errors.WithMessage(err, "failed to convert to eth log")
			}
			responseSize.add(ethLogJSONSize(log))
			if useBoundCheck && responseSize.size() > maxGetLogsResponseBytes {
				return nil, false, handler.newSuggestedBodyBytesOversizedError(filter, v.BlockNumber)
			}
			logs = append(logs, *log)
		}
	}

	// query data from fullnode
	if fnFilter != nil {
		// check timeout before fullnode delegation
		if err := checkTimeout(ctx); err != nil {
			return nil, false, err
		}

		// ensure fullnode delegation is rational
		if err := handler.checkFnEthLogFilter(fnFilter); err != nil {
			return nil, false, err
		}

		fence, err := handler.newLogsFence(eth, dbFilter)
		if err != nil {
			return nil, false, err
		}
		if err := fence.check(); err != nil {
			return nil, false, err
		}

		fnLogs, err := eth.Logs(*fnFilter)
		if err != nil {
			return nil, false, err
		}

		if err := fence.check(); err != nil {
			return nil, false, err
		}

		for i := range fnLogs {
			responseSize.add(ethLogJSONSize(&fnLogs[i]))
			if useBoundCheck && responseSize.size() > maxGetLogsResponseBytes {
				return nil, false, handler.newSuggestedBodyBytesOversizedError(filter, fnLogs[i].BlockNumber)
			}
		}
		logs = append(logs, fnLogs...)
	}

	// ensure result set never oversized
	if useBoundCheck && uint64(len(logs)) > store.MaxLogLimit {
		exceedingBlockNum := logs[store.MaxLogLimit].BlockNumber
		return nil, false, handler.newSuggestedResultSetOversizedError(filter, exceedingBlockNum)
	}

	// Rare case: log context information for diagnostic purposes if the result exceeds limits.
	if uint64(len(logs)) > store.MaxLogLimit || responseSize.size() > maxGetLogsResponseBytes {
		logrus.WithFields(logrus.Fields{
			"logFilter":         filter,
			"databaseFilter":    dbFilter,
			"fullnodeFilter":    fnFilter,
			"boundCheckEnabled": useBoundCheck,
			"resultSetCount":    len(logs),
			"responseSizeBytes": responseSize.size(),
		}).Info("Exceeded limits for getLogs response")
	}

	return logs, dbFilter != nil, nil
}

func (handler *EthLogsApiHandler) splitLogFilter(
	eth ethLogsClient,
	filter *types.FilterQuery,
) (*store.LogFilter, *types.FilterQuery, error) {
	earliestMapping, ok, err := handler.es.EarliestBlockMapping()
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, filter, nil
	}

	latestMapping, ok, err := handler.es.LatestBlockMapping()
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, filter, nil
	}

	if filter.BlockHash != nil {
		return handler.splitLogFilterByBlockHash(eth, filter, earliestMapping.BnMin, latestMapping.BnMax)
	}

	return handler.splitLogFilterByBlockRange(filter, earliestMapping.BnMin, latestMapping.BnMax)
}

func (handler *EthLogsApiHandler) splitLogFilterByBlockHash(
	eth ethBlockByHashClient,
	filter *types.FilterQuery,
	minBlock uint64,
	maxBlock uint64,
) (*store.LogFilter, *types.FilterQuery, error) {
	block, err := eth.BlockByHash(*filter.BlockHash, false)
	if err != nil {
		return nil, nil, err
	}

	// Block lookup and getLogs use different Fullnode paths. Let getLogs decide
	// the result and preserve its RPC error, including changes between calls.
	if block == nil {
		metricutil.GetOrRegisterMeter("infura/rpc/eth_getLogs/hash/fallback/unknown").Mark(1)
		return nil, filter, nil
	}
	if block.Number == nil || block.Number.Sign() < 0 || !block.Number.IsUint64() {
		return nil, nil, errors.New("invalid block")
	}

	bn := block.Number.Uint64()
	if bn < minBlock {
		return nil, nil, store.ErrAlreadyPruned
	}

	if bn > maxBlock {
		return nil, filter, nil
	}

	pivot, ok, err := handler.es.PivotHash(bn)
	if err != nil {
		return nil, nil, err
	}
	if !ok || !strings.EqualFold(pivot, filter.BlockHash.Hex()) {
		metricutil.GetOrRegisterMeter("infura/rpc/eth_getLogs/hash/fallback/identity").Mark(1)
		return nil, filter, nil
	}
	dbFilter := store.ParseEthLogFilter(bn, bn, filter)
	return &dbFilter, nil, err
}

func (handler *EthLogsApiHandler) splitLogFilterByBlockRange(
	filter *types.FilterQuery,
	minBlock uint64,
	maxBlock uint64,
) (*store.LogFilter, *types.FilterQuery, error) {
	if filter.FromBlock == nil || *filter.FromBlock < 0 {
		return nil, filter, nil
	}

	if filter.ToBlock == nil || *filter.ToBlock < 0 {
		return nil, filter, nil
	}

	blockFrom, blockTo := uint64(*filter.FromBlock), uint64(*filter.ToBlock)
	if blockFrom < minBlock {
		return nil, nil, store.ErrAlreadyPruned
	}

	// no data in database
	if blockFrom > maxBlock {
		return nil, filter, nil
	}

	// all data in database
	if blockTo <= maxBlock {
		dbFilter := store.ParseEthLogFilter(blockFrom, blockTo, filter)
		return &dbFilter, nil, nil
	}

	// otherwise, partial data in database
	dbFilter := store.ParseEthLogFilter(blockFrom, maxBlock, filter)
	fnBlockFrom := types.BlockNumber(maxBlock + 1)
	fnFilter := types.FilterQuery{
		FromBlock: &fnBlockFrom,
		ToBlock:   filter.ToBlock,
		Addresses: filter.Addresses,
		Topics:    filter.Topics,
	}

	return &dbFilter, &fnFilter, nil
}

// checkFnEthLogFilter checks if the eth log filter is rational for fullnode delegation.
//
// Note this function assumes the log filter is valid and normalized.
func (handler *EthLogsApiHandler) checkFnEthLogFilter(filter *types.FilterQuery) error {
	if blockRange, valid := calculateEthBlockRange(filter); valid {
		numBlocks := blockRange.To - blockRange.From + 1
		if numBlocks > uint64(store.MaxLogBlockRange) {
			blockRange.To = blockRange.From + uint64(store.MaxLogBlockRange) - 1
			suggestedRange := store.SuggestedBlockRange{RangeUint64: blockRange}
			return store.NewSuggestedFilterQuerySetTooLargeError(&suggestedRange)
		}
	}

	return nil
}

// RequiresBoundChecks determines if bound checks should be applied based on if there is any space to narrow down the log filter
func (handler *EthLogsApiHandler) RequiresBoundChecks(filter *types.FilterQuery) bool {
	if filter.FromBlock != nil && filter.ToBlock != nil {
		return *filter.FromBlock < *filter.ToBlock
	}
	return false
}

func (handler *EthLogsApiHandler) newSuggestedResultSetOversizedError(filter *types.FilterQuery, exceedingBlockNum uint64) error {
	return handler.newSuggestedFilterOversizedError(store.ErrFilterResultSetTooLarge, filter, exceedingBlockNum)
}

func (handler *EthLogsApiHandler) newSuggestedBodyBytesOversizedError(filter *types.FilterQuery, exceedingBlockNum uint64) error {
	return handler.newSuggestedFilterOversizedError(errResponseBodySizeTooLarge, filter, exceedingBlockNum)
}

func (handler *EthLogsApiHandler) newSuggestedFilterOversizedError(inner error, filter *types.FilterQuery, exceedingBlockNum uint64) error {
	if filter.FromBlock != nil {
		fromBlock := uint64(*filter.FromBlock)
		if exceedingBlockNum > fromBlock {
			return store.NewSuggestedFilterOversizeError(inner, store.NewSuggestedBlockRange(fromBlock, exceedingBlockNum-1, 0))
		}
	}

	return inner
}

// calculateEthBlockRange calculates the block range of the log filter and returns the gap and a boolean indicating success.
func calculateEthBlockRange(filter *types.FilterQuery) (blockRange citypes.RangeUint64, ok bool) {
	if filter == nil || filter.FromBlock == nil || filter.ToBlock == nil {
		return blockRange, false
	}

	if *filter.FromBlock > *filter.ToBlock {
		return blockRange, false
	}

	blockRange.From, blockRange.To = uint64(*filter.FromBlock), uint64(*filter.ToBlock)
	return blockRange, true
}
