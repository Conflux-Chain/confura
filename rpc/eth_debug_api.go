package rpc

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
)

type ethDebugAPI struct{}

func (api *ethDebugAPI) TraceTransaction(
	ctx context.Context, txnHash common.Hash, opts ...*types.GethDebugTracingOptions) (*types.GethTrace, error) {
	return GetEthClientFromContext(ctx).Debug.TraceTransaction(txnHash, opts...)
}

func (api *ethDebugAPI) TraceBlockByHash(
	ctx context.Context, blockHash common.Hash, opts ...*types.GethDebugTracingOptions) ([]*types.GethTraceResult, error) {
	return GetEthClientFromContext(ctx).Debug.TraceBlockByHash(blockHash, opts...)
}

func (api *ethDebugAPI) TraceBlockByNumber(
	ctx context.Context, blockNumber types.BlockNumber, opts ...*types.GethDebugTracingOptions) ([]*types.GethTraceResult, error) {
	return GetEthClientFromContext(ctx).Debug.TraceBlockByNumber(blockNumber, opts...)
}

func (api *ethDebugAPI) TraceCall(
	ctx context.Context, request types.CallRequest, blockNumber *types.BlockNumber, opts ...*types.GethDebugTracingOptions,
) (*types.GethTrace, error) {
	return GetEthClientFromContext(ctx).Debug.TraceCall(request, blockNumber, opts...)
}
