package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
)

type ethDebugAPI struct {
	stateHandler *handler.EthStateHandler
}

func (api *ethDebugAPI) TraceTransaction(
	ctx context.Context, txnHash common.Hash, opts ...*types.GethDebugTracingOptions) (*types.GethTrace, error) {
	w3c := GetEthClientFromContext(ctx)
	return api.stateHandler.DebugTraceTransaction(ctx, w3c, txnHash, opts...)
}

func (api *ethDebugAPI) TraceBlockByHash(
	ctx context.Context, blockHash common.Hash, opts ...*types.GethDebugTracingOptions) ([]*types.GethTraceResult, error) {
	w3c := GetEthClientFromContext(ctx)
	return api.stateHandler.DebugTraceBlockByHash(ctx, w3c, blockHash, opts...)
}

func (api *ethDebugAPI) TraceBlockByNumber(
	ctx context.Context, blockNumber types.BlockNumber, opts ...*types.GethDebugTracingOptions) ([]*types.GethTraceResult, error) {
	w3c := GetEthClientFromContext(ctx)
	return api.stateHandler.DebugTraceBlockByNumber(ctx, w3c, blockNumber, opts...)
}

func (api *ethDebugAPI) TraceCall(
	ctx context.Context, request types.CallRequest, blockNumber *types.BlockNumber, opts ...*types.GethDebugTracingOptions,
) (*types.GethTrace, error) {
	w3c := GetEthClientFromContext(ctx)
	return api.stateHandler.DebugTraceCall(ctx, w3c, request, blockNumber, opts...)
}
