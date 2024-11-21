package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

// traceAPI provides core space trace RPC proxy API.
type traceAPI struct {
	stateHandler *handler.CfxStateHandler
}

func (api *traceAPI) Block(ctx context.Context, blockHash types.Hash) (*types.LocalizedBlockTrace, error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.TraceBlock(ctx, cfx, blockHash)
}

func (api *traceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.TraceFilter(ctx, cfx, filter)
}

func (api *traceAPI) Transaction(ctx context.Context, txHash types.Hash) ([]types.LocalizedTrace, error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.TraceTransaction(ctx, cfx, txHash)
}

func (api *traceAPI) Epoch(ctx context.Context, epoch types.Epoch) (types.EpochTrace, error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.TraceEpoch(ctx, cfx, epoch)
}
