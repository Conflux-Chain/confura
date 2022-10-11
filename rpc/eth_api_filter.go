package rpc

import (
	"context"
	"errors"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
	web3Types "github.com/openweb3/web3go/types"
)

// NewFilter creates a new filter and returns the filter id. It can be
// used to retrieve logs when the state changes. This method cannot be
// used to fetch logs that are already stored in the state.
//
// Default criteria for the from and to block are "latest".
// Using "latest" as block number will return logs for mined blocks.
// Using "pending" as block number returns logs for not yet mined (pending) blocks.
// In case logs are removed (chain reorg) previously returned logs are returned
// again but with the removed property set to true.
//
// In case "fromBlock" > "toBlock" an error is returned.
func (api *ethAPI) NewFilter(ctx context.Context, filter web3Types.FilterQuery) (rpc.ID, error) {
	w3c := GetEthClientFromContext(ctx)
	// TODO: integrate golang sdk
	_ = w3c
	return "", errors.New("not supported yet")
}

// NewBlockFilter creates a filter that fetches blocks that are imported into the chain.
// It is part of the filter package since polling goes with eth_getFilterChanges.
func (api *ethAPI) NewBlockFilter(ctx context.Context) (rpc.ID, error) {
	w3c := GetEthClientFromContext(ctx)
	// TODO: integrate golang sdk
	_ = w3c
	return "", errors.New("not supported yet")
}

// NewPendingTransactionFilter creates a filter that fetches pending transaction hashes
// as transactions enter the pending state.
//
// It is part of the filter package because this filter can be used through the
// `eth_getFilterChanges` polling method that is also used for log filters.
func (api *ethAPI) NewPendingTransactionFilter(ctx context.Context) (rpc.ID, error) {
	w3c := GetEthClientFromContext(ctx)
	// TODO: integrate golang sdk
	_ = w3c
	return "", errors.New("not supported yet")
}

// UninstallFilter removes the filter with the given filter id.
func (api *ethAPI) UninstallFilter(ctx context.Context, id rpc.ID) (bool, error) {
	w3c := GetEthClientFromContext(ctx)
	// TODO: integrate golang sdk
	_ = w3c
	return false, errors.New("not supported yet")
}

// GetFilterChanges returns the logs for the filter with the given id since
// last time it was called. This can be used for polling.
//
// For pending transaction and block filters the result is []common.Hash.
// (pending)Log filters return []Log.
func (api *ethAPI) GetFilterChanges(ctx context.Context, id rpc.ID) (interface{}, error) {
	w3c := GetEthClientFromContext(ctx)
	// TODO: integrate golang sdk
	_ = w3c
	return false, errors.New("not supported yet")
}

// GetFilterLogs returns the logs for the filter with the given id.
// If the filter could not be found an empty array of logs is returned.
func (api *ethAPI) GetFilterLogs(ctx context.Context, id rpc.ID) ([]*types.Log, error) {
	w3c := GetEthClientFromContext(ctx)
	// TODO: integrate golang sdk
	_ = w3c
	return nil, errors.New("not supported yet")
}
