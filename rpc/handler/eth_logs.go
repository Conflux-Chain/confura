package handler

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/Conflux-Chain/confura/rpc/ethbridge"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
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
) ([]types.Log, bool, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, store.TimeoutGetLogs)
	defer cancel()

	// record the reorg version before query to ensure data consistence
	lastReorgVersion, err := handler.ms.GetReorgVersion()
	if err != nil {
		return nil, false, err
	}

	for {
		logs, hitStore, err := handler.getLogsReorgGuard(timeoutCtx, eth, filter)
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
) ([]types.Log, bool, error) {
	// Try to query event logs from database and fullnode.
	dbFilter, fnFilter, err := handler.splitLogFilter(eth, filter)
	if err != nil {
		return nil, false, err
	}

	metrics.Registry.RPC.Percentage("eth_getLogs", "filter/split/alldatabase").Mark(fnFilter == nil)
	metrics.Registry.RPC.Percentage("eth_getLogs", "filter/split/allfullnode").Mark(dbFilter == nil)
	metrics.Registry.RPC.Percentage("eth_getLogs", "filter/split/partial").Mark(dbFilter != nil && fnFilter != nil)

	var logs []types.Log

	// query data from database
	if dbFilter != nil {
		dbLogs, err := handler.ms.GetLogs(ctx, *dbFilter)
		if err != nil {
			// TODO ErrPrunedAlready
			return nil, false, err
		}

		for _, v := range dbLogs {
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
		if err := handler.checkFullnodeLogFilter(fnFilter); err != nil {
			return nil, false, err
		}

		fnLogs, err := eth.Logs(*fnFilter)
		if err != nil {
			return nil, false, err
		}

		logs = append(logs, fnLogs...)
	}

	if len(logs) > int(store.MaxLogLimit) {
		return nil, false, store.ErrGetLogsResultSetTooLarge
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

	if block == nil {
		return nil, nil, errors.New("unknown block")
	}

	bn := block.Number.Uint64()

	if bn > maxBlock {
		return nil, filter, nil
	}

	networkId, err := handler.getNetworkId(eth)
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

	networkId, err := handler.getNetworkId(eth)
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

func (handler *EthLogsApiHandler) getNetworkId(eth *client.RpcEthClient) (uint32, error) {
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

// checkFullnodeLogFilter checks if the log filter is rational for fullnode delegation.
//
// Note this function assumes the log filter is valid and normalized.
func (handler *EthLogsApiHandler) checkFullnodeLogFilter(filter *types.FilterQuery) error {
	if filter.FromBlock != nil && filter.ToBlock != nil {
		count := *filter.ToBlock - *filter.FromBlock + 1
		if uint64(count) > store.MaxLogEpochRange {
			return store.ErrGetLogsQuerySetTooLarge
		}
	}

	return nil
}
