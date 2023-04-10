package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
)

const (
	rpcMethodCfxNewFilter = "eth_newFilter"
)

// NewFilter creates a new filter and returns the filter id. It can be
// used to retrieve logs when the state changes. This method cannot be
// used to fetch logs that are already stored in the state.
func (api *cfxAPI) NewFilter(ctx context.Context, filterCrit types.LogFilter) (*rpc.ID, error) {
	cfx := GetCfxClientFromContext(ctx)
	metrics.UpdateCfxRpcLogFilter(rpcMethodCfxNewFilter, cfx, &filterCrit)

	fid, err := api.VirtualFilterClient.NewFilter(cfx.GetNodeURL(), &filterCrit)
	return fid, errVirtualFilterProxyErrorOrNil(err)
}

// NewBlockFilter creates a filter that fetches blocks that are imported into the chain.
// It is part of the filter package since polling goes with cfx_getFilterChanges.
func (api *cfxAPI) NewBlockFilter(ctx context.Context) (*rpc.ID, error) {
	cfx := GetCfxClientFromContext(ctx)

	fid, err := api.VirtualFilterClient.NewBlockFilter(cfx.GetNodeURL())
	return fid, errVirtualFilterProxyErrorOrNil(err)
}

// NewPendingTransactionFilter creates a filter that fetches pending transaction hashes
// as transactions enter the pending state.
//
// It is part of the filter package because this filter can be used through the
// `cfx_getFilterChanges` polling method that is also used for log filters.
func (api *cfxAPI) NewPendingTransactionFilter(ctx context.Context) (*rpc.ID, error) {
	cfx := GetCfxClientFromContext(ctx)

	fid, err := api.VirtualFilterClient.NewPendingTransactionFilter(cfx.GetNodeURL())
	return fid, errVirtualFilterProxyErrorOrNil(err)
}

// UninstallFilter removes the filter with the given filter id.
func (api *cfxAPI) UninstallFilter(ctx context.Context, fid rpc.ID) (bool, error) {
	ok, err := api.VirtualFilterClient.UninstallFilter(fid)
	return ok, errVirtualFilterProxyErrorOrNil(err)
}

// GetFilterChanges returns the logs for the filter with the given id since
// last time it was called. This can be used for polling.
//
// For pending transaction and block filters the result is []types.Hash.
// (pending)Log filters return []types.CfxFilterLog.
func (api *cfxAPI) GetFilterChanges(ctx context.Context, fid rpc.ID) (interface{}, error) {
	res, err := api.VirtualFilterClient.GetFilterChanges(fid)
	return res, errVirtualFilterProxyErrorOrNil(err)
}

// GetFilterLogs returns the logs for the filter with the given id.
// If the filter could not be found an empty array of logs is returned.
func (api *cfxAPI) GetFilterLogs(ctx context.Context, fid rpc.ID) ([]types.Log, error) {
	logs, err := api.VirtualFilterClient.GetFilterLogs(fid)
	return logs, errVirtualFilterProxyErrorOrNil(err)
}
