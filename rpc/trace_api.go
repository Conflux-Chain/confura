package rpc

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

// traceAPI provides core space trace RPC proxy API.
type traceAPI struct{}

func (api *traceAPI) Block(ctx context.Context, blockHash types.Hash) (*types.LocalizedBlockTrace, error) {
	return GetCfxClientFromContext(ctx).Trace().GetBlockTraces(blockHash)
}

func (api *traceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	return GetCfxClientFromContext(ctx).Trace().FilterTraces(filter)
}

func (api *traceAPI) Transaction(ctx context.Context, txHash types.Hash) ([]types.LocalizedTrace, error) {
	return GetCfxClientFromContext(ctx).Trace().GetTransactionTraces(txHash)
}

func (api *traceAPI) Epoch(ctx context.Context, epoch types.Epoch) ([]types.LocalizedTrace, error) {
	return GetCfxClientFromContext(ctx).Trace().GetEpochTraces(epoch)
}
