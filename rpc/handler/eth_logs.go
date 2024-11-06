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
)

// EthLogsApiHandler RPC handler to get evm space event logs from store or fullnode.
type EthLogsApiHandler struct {
	ms *mysql.MysqlStore

	networkId atomic.Value
}

func NewEthLogsApiHandler(ms *mysql.MysqlStore) *EthLogsApiHandler {
	return &EthLogsApiHandler{ms: ms}
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

	for {
		logs, hitStore, err := handler.getLogsReorgGuard(ctx, eth, filter, delegatedRpcMethod)
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

	// query data from database
	if dbFilter != nil {
		// add db query timeout
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, store.TimeoutGetLogs)
		defer cancel()

		dbLogs, err := handler.ms.GetLogs(ctx, *dbFilter)
		if err != nil {
			// TODO ErrPrunedAlready
			return nil, false, err
		}

		for _, v := range dbLogs {
			if accumulator, err = handler.accumulateBodySizeOfLogs(filter, accumulator, v); err != nil {
				return nil, false, err
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
			if accumulator, err = handler.accumulateBodySizeOfEthLogs(filter, accumulator, fnLogs[i]); err != nil {
				return nil, false, err
			}
		}
		logs = append(logs, fnLogs...)
	}

	if len(logs) > int(store.MaxLogLimit) {
		return nil, false, store.ErrFilterResultSetTooLarge
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

// Accumulate body size and suggest range if exceeded
func (handler *EthLogsApiHandler) accumulateBodySizeOfLogs(filter *types.FilterQuery, accumulator int, logs ...*store.Log) (int, error) {
	for _, log := range logs {
		accumulator += len(log.Extra)
		if uint64(accumulator) > maxGetLogsResponseBytes {
			return accumulator, newEthSuggestedBodyBytesOversizedError(filter, log.BlockNumber)
		}
	}
	return accumulator, nil
}

// Accumulate body size and suggest range if exceeded for CfxLogs
func (handler *EthLogsApiHandler) accumulateBodySizeOfEthLogs(filter *types.FilterQuery, accumulator int, logs ...types.Log) (int, error) {

	for _, log := range logs {
		accumulator += len(log.Data)
		if uint64(accumulator) > maxGetLogsResponseBytes {
			return accumulator, newEthSuggestedBodyBytesOversizedError(filter, log.BlockNumber)
		}
	}
	return accumulator, nil
}

func newEthSuggestedBodyBytesOversizedError(filter *types.FilterQuery, firstExceedingBlockNum uint64) error {
	if filter.FromBlock == nil {
		return errResponseBodySizeTooLarge
	}

	fromBlock := uint64(*filter.FromBlock)
	if firstExceedingBlockNum > fromBlock {
		return store.NewSuggestedFilterOversizeError(
			errResponseBodySizeTooLarge,
			store.NewSuggestedBlockRange(fromBlock, firstExceedingBlockNum-1, 0),
		)
	}

	return errResponseBodySizeTooLarge
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
