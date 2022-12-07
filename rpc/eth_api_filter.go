package rpc

import (
	"context"
	"errors"

	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/go-rpc-provider/utils"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

const (
	rpcMethodEthNewFilter = "eth_newFilter"
)

var (
	// uniform virtual filter connection error
	errVirtualFilterConnError = errors.New("virtual filter connection error")
)

// NewFilter creates a new filter and returns the filter id. It can be
// used to retrieve logs when the state changes. This method cannot be
// used to fetch logs that are already stored in the state.
//
// Default criteria for the from and to block are "latest".
// Using "latest" as block number will return logs for mined blocks.
// Using "pending" as block number is not supported.
// In case logs are removed (chain reorg) previously returned logs are returned
// again but with the removed property set to true.
//
// In case "fromBlock" > "toBlock" an error is returned.
func (api *ethAPI) NewFilter(ctx context.Context, fq web3Types.FilterQuery) (*rpc.ID, error) {
	w3c := GetEthClientFromContext(ctx)
	metrics.UpdateEthRpcLogFilter(rpcMethodEthNewFilter, w3c.Eth, &fq)

	fid, err := api.VirtualFilterClient.NewFilter(w3c.URL, &fq)
	if err != nil && !utils.IsRPCJSONError(err) {
		logrus.WithError(err).Error("Failed to connect virtual filter for new log filter")
		return fid, errVirtualFilterConnError
	}

	return fid, err
}

// NewBlockFilter creates a filter that fetches blocks that are imported into the chain.
// It is part of the filter package since polling goes with eth_getFilterChanges.
func (api *ethAPI) NewBlockFilter(ctx context.Context) (*rpc.ID, error) {
	w3c := GetEthClientFromContext(ctx)
	fid, err := api.VirtualFilterClient.NewBlockFilter(w3c.URL)
	if err != nil && !utils.IsRPCJSONError(err) {
		logrus.WithError(err).Error("Failed to connect virtual filter for new block filter")
		return fid, errVirtualFilterConnError
	}

	return fid, err
}

// NewPendingTransactionFilter creates a filter that fetches pending transaction hashes
// as transactions enter the pending state.
//
// It is part of the filter package because this filter can be used through the
// `eth_getFilterChanges` polling method that is also used for log filters.
func (api *ethAPI) NewPendingTransactionFilter(ctx context.Context) (*rpc.ID, error) {
	w3c := GetEthClientFromContext(ctx)
	fid, err := api.VirtualFilterClient.NewPendingTransactionFilter(w3c.URL)
	if err != nil && !utils.IsRPCJSONError(err) {
		logrus.WithError(err).Error("Failed to connect virtual filter for new pending txn filter")
		return fid, errVirtualFilterConnError
	}

	return fid, err
}

// UninstallFilter removes the filter with the given filter id.
func (api *ethAPI) UninstallFilter(ctx context.Context, fid rpc.ID) (bool, error) {
	ok, err := api.VirtualFilterClient.UninstallFilter(fid)
	if err != nil && !utils.IsRPCJSONError(err) {
		logrus.WithError(err).Error("Failed to connect virtual filter to uninstall filter")
		return ok, errVirtualFilterConnError
	}

	return ok, err
}

// GetFilterChanges returns the logs for the filter with the given id since
// last time it was called. This can be used for polling.
//
// For pending transaction and block filters the result is []common.Hash.
// (pending)Log filters return []Log.
func (api *ethAPI) GetFilterChanges(ctx context.Context, fid rpc.ID) (interface{}, error) {
	res, err := api.VirtualFilterClient.GetFilterChanges(fid)
	if err != nil && !utils.IsRPCJSONError(err) {
		logrus.WithError(err).Error("Failed to connect virtual filter to get filter changes")
		return res, errVirtualFilterConnError
	}

	return res, err
}

// GetFilterLogs returns the logs for the filter with the given id.
// If the filter could not be found an empty array of logs is returned.
func (api *ethAPI) GetFilterLogs(ctx context.Context, fid rpc.ID) ([]web3Types.Log, error) {
	logs, err := api.VirtualFilterClient.GetFilterLogs(fid)
	if err != nil && !utils.IsRPCJSONError(err) {
		logrus.WithError(err).Error("Failed to connect virtual filter to get filter logs")
		return logs, errVirtualFilterConnError
	}

	return logs, err
}
