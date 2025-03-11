package handler

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/Conflux-Chain/confura/rpc/ethbridge"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/openweb3/web3go/client"
	"github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// EthLogsApiHandler RPC handler to get evm space event logs from store or fullnode.
type EthLogsApiHandler struct {
	ms *mysql.MysqlStore

	networkId          atomic.Value
	maxSuggestAttempts int
}

func NewEthLogsApiHandler(ms *mysql.MysqlStore) *EthLogsApiHandler {
	maxSuggestAttempts := viper.GetInt("requestControl.maxGetLogsSuggestionAttempts")
	return &EthLogsApiHandler{
		ms: ms, maxSuggestAttempts: maxSuggestAttempts,
	}
}

func (handler *EthLogsApiHandler) GetLogs(
	ctx context.Context,
	eth *client.RpcEthClient,
	filter *types.FilterQuery,
	delegatedRpcMethod string,
) ([]types.Log, bool, error) {
	// record the reorg version before query to ensure data consistence
	lastReorgVersion, err := handler.ms.GetReorgVersion()
	if err != nil {
		return nil, false, err
	}

	var suggestErr *store.SuggestedFilterOversizedError[store.SuggestedBlockRange]
	suggestCount := 0

	for {
		logs, hitStore, err := handler.getLogsReorgGuard(ctx, eth, filter, delegatedRpcMethod)
		if err != nil {
			if maxAttempts := handler.maxSuggestAttempts; maxAttempts > 0 && suggestCount < maxAttempts &&
				filter.ToBlock != nil && errors.As(err, &suggestErr) {
				*filter.ToBlock = types.BlockNumber(suggestErr.SuggestedRange.To)
				suggestCount++
				continue
			}
			if suggestErr != nil && !errors.Is(err, store.ErrGetLogsTimeout) {
				return nil, false, suggestErr
			}
			return nil, false, err
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

func (handler *EthLogsApiHandler) getLogsReorgGuard(
	ctx context.Context,
	eth *client.RpcEthClient,
	filter *types.FilterQuery,
	delegatedRpcMethod string,
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
	var accumulator int

	useBoundCheck := handler.RequiresBoundChecks(filter)
	if dbFilter != nil {
		if useBoundCheck {
			// add db query timeout
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, store.TimeoutGetLogs)
			defer cancel()
		} else {
			ctx = store.NewContextWithBoundChecksDisabled(ctx)
		}

		// query data from database
		dbLogs, err := handler.ms.GetLogs(ctx, *dbFilter)
		if err != nil {
			// TODO ErrPrunedAlready
			return nil, false, err
		}

		for _, v := range dbLogs {
			if accumulator += len(v.Extra); useBoundCheck && uint64(accumulator) > maxGetLogsResponseBytes {
				return nil, false, handler.newSuggestedBodyBytesOversizedError(filter, v.BlockNumber)
			}

			cfxLog, ext := v.ToCfxLog()
			logs = append(logs, *ethbridge.ConvertLog(cfxLog, ext))
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

		fnLogs, err := eth.Logs(*fnFilter)
		if err != nil {
			return nil, false, err
		}

		for i := range fnLogs {
			if accumulator += len(fnLogs[i].Data); useBoundCheck && uint64(accumulator) > maxGetLogsResponseBytes {
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
	if uint64(len(logs)) > store.MaxLogLimit || uint64(accumulator) > maxGetLogsResponseBytes {
		logrus.WithFields(logrus.Fields{
			"logFilter":         filter,
			"databaseFilter":    dbFilter,
			"fullnodeFilter":    fnFilter,
			"boundCheckEnabled": useBoundCheck,
			"resultSetCount":    len(logs),
			"responseSizeBytes": uint64(accumulator),
		}).Info("Exceeded limits for getLogs response")
	}

	return logs, dbFilter != nil, nil
}

func (handler *EthLogsApiHandler) splitLogFilter(
	eth *client.RpcEthClient,
	filter *types.FilterQuery,
) (*store.LogFilter, *types.FilterQuery, error) {
	maxBlock, ok, err := handler.ms.MaxEpoch()
	if err != nil {
		return nil, nil, err
	}

	if !ok {
		return nil, filter, nil
	}

	if filter.BlockHash != nil {
		return handler.splitLogFilterByBlockHash(eth, filter, maxBlock)
	}

	return handler.splitLogFilterByBlockRange(eth, filter, maxBlock)
}

func (handler *EthLogsApiHandler) splitLogFilterByBlockHash(
	eth *client.RpcEthClient,
	filter *types.FilterQuery,
	maxBlock uint64,
) (*store.LogFilter, *types.FilterQuery, error) {
	block, err := eth.BlockByHash(*filter.BlockHash, false)
	if err != nil {
		return nil, nil, err
	}

	if block == nil || block.Number == nil {
		return nil, nil, errors.New("unknown block")
	}

	bn := block.Number.Uint64()

	if bn > maxBlock {
		return nil, filter, nil
	}

	networkId, err := handler.GetNetworkId(eth)
	if err != nil {
		return nil, nil, err
	}

	dbFilter := store.ParseEthLogFilter(bn, bn, filter, networkId)
	return &dbFilter, nil, err
}

func (handler *EthLogsApiHandler) splitLogFilterByBlockRange(
	eth *client.RpcEthClient,
	filter *types.FilterQuery,
	maxBlock uint64,
) (*store.LogFilter, *types.FilterQuery, error) {
	if filter.FromBlock == nil || *filter.FromBlock < 0 {
		return nil, filter, nil
	}

	if filter.ToBlock == nil || *filter.ToBlock < 0 {
		return nil, filter, nil
	}

	blockFrom, blockTo := uint64(*filter.FromBlock), uint64(*filter.ToBlock)

	// no data in database
	if blockFrom > maxBlock {
		return nil, filter, nil
	}

	networkId, err := handler.GetNetworkId(eth)
	if err != nil {
		return nil, nil, err
	}

	// all data in database
	if blockTo <= maxBlock {
		dbFilter := store.ParseEthLogFilter(blockFrom, blockTo, filter, networkId)
		return &dbFilter, nil, nil
	}

	// otherwise, partial data in databse
	dbFilter := store.ParseEthLogFilter(blockFrom, maxBlock, filter, networkId)
	fnBlockFrom := types.BlockNumber(maxBlock + 1)
	fnFilter := types.FilterQuery{
		FromBlock: &fnBlockFrom,
		ToBlock:   filter.ToBlock,
		Addresses: filter.Addresses,
		Topics:    filter.Topics,
	}

	return &dbFilter, &fnFilter, nil
}

func (handler *EthLogsApiHandler) GetNetworkId(eth *client.RpcEthClient) (uint32, error) {
	if val := handler.networkId.Load(); val != nil {
		return val.(uint32), nil
	}

	chainId, err := eth.ChainId()
	if err != nil {
		return 0, err
	}

	networkId := uint32(*chainId)
	handler.networkId.Store(networkId)

	return networkId, nil
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
