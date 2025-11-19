package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/util/metrics"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
)

const (
	rpcMethodCfxNewFilter     = "cfx_newFilter"
	rpcMethodCfxGetFilterLogs = "cfx_getFilterLogs"
)

func isCfxFilterRpcMethod(method string) bool {
	switch method {
	case "cfx_newFilter", "cfx_newBlockFilter", "cfx_newPendingTransactionFilter":
		return true
	case "cfx_getFilterChanges", "cfx_getFilterLogs", "cfx_uninstallFilter":
		return true
	default:
		return false
	}
}

// NewFilter creates a new filter and returns the filter id. It can be
// used to retrieve logs when the state changes. This method cannot be
// used to fetch logs that are already stored in the state.
func (api *cfxAPI) NewFilter(ctx context.Context, filterCrit types.LogFilter) (*rpc.ID, error) {
	cfx := GetCfxClientFromContext(ctx)
	metrics.UpdateCfxRpcLogFilter(rpcMethodCfxNewFilter, cfx, &filterCrit)

	if api.VirtualFilterClient != nil {
		fid, err := api.VirtualFilterClient.NewFilter(cfx.GetNodeURL(), &filterCrit)
		return fid, errVirtualFilterProxyErrorOrNil(err)
	}

	return cfx.(*sdk.Client).Filter().NewFilter(filterCrit)
}

// NewBlockFilter creates a filter that fetches blocks that are imported into the chain.
// It is part of the filter package since polling goes with cfx_getFilterChanges.
func (api *cfxAPI) NewBlockFilter(ctx context.Context) (*rpc.ID, error) {
	cfx := GetCfxClientFromContext(ctx)

	if api.VirtualFilterClient != nil {
		fid, err := api.VirtualFilterClient.NewBlockFilter(cfx.GetNodeURL())
		return fid, errVirtualFilterProxyErrorOrNil(err)
	}

	return cfx.(*sdk.Client).Filter().NewBlockFilter()
}

// NewPendingTransactionFilter creates a filter that fetches pending transaction hashes
// as transactions enter the pending state.
//
// It is part of the filter package because this filter can be used through the
// `cfx_getFilterChanges` polling method that is also used for log filters.
func (api *cfxAPI) NewPendingTransactionFilter(ctx context.Context) (*rpc.ID, error) {
	cfx := GetCfxClientFromContext(ctx)

	if api.VirtualFilterClient != nil {
		fid, err := api.VirtualFilterClient.NewPendingTransactionFilter(cfx.GetNodeURL())
		return fid, errVirtualFilterProxyErrorOrNil(err)
	}

	return cfx.(*sdk.Client).Filter().NewPendingTransactionFilter()
}

// UninstallFilter removes the filter with the given filter id.
func (api *cfxAPI) UninstallFilter(ctx context.Context, fid rpc.ID) (bool, error) {
	if api.VirtualFilterClient != nil {
		ok, err := api.VirtualFilterClient.UninstallFilter(fid)
		return ok, errVirtualFilterProxyErrorOrNil(err)
	}

	cfx := GetCfxClientFromContext(ctx)
	return cfx.(*sdk.Client).Filter().UninstallFilter(fid)
}

// GetFilterChanges returns the logs for the filter with the given id since
// last time it was called. This can be used for polling.
//
// For pending transaction and block filters the result is []types.Hash.
// (pending)Log filters return []types.CfxFilterLog.
func (api *cfxAPI) GetFilterChanges(ctx context.Context, fid rpc.ID) (any, error) {
	if api.VirtualFilterClient != nil {
		res, err := api.VirtualFilterClient.GetFilterChanges(fid)
		return res, errVirtualFilterProxyErrorOrNil(err)
	}

	cfx := GetCfxClientFromContext(ctx)
	return cfx.(*sdk.Client).Filter().GetFilterChanges(fid)
}

// GetFilterLogs returns the logs for the filter with the given id.
// If the filter could not be found an empty array of logs is returned.
func (api *cfxAPI) GetFilterLogs(ctx context.Context, fid rpc.ID) ([]types.Log, error) {
	if api.VirtualFilterClient == nil {
		cfx := GetCfxClientFromContext(ctx)
		return cfx.(*sdk.Client).Filter().GetFilterLogs(fid)
	}

	fq, err := api.VirtualFilterClient.GetLogFilter(fid)
	if err != nil {
		return emptyLogs, errVirtualFilterProxyErrorOrNil(err)
	}

	cfx, err := api.provider.GetClientByIP(ctx, node.GroupCfxLogs)
	if err != nil {
		return emptyLogs, errors.WithMessage(err, "failed to get client by ip")
	}

	return api.getLogs(ctx, cfx, *fq, rpcMethodCfxGetFilterLogs)
}

func uniformCfxLogs(logs []types.Log) []types.Log {
	if logs == nil {
		return emptyLogs
	}

	return logs
}
