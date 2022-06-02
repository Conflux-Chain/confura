package rpc

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

type traceAPI struct{}

func (api *traceAPI) Block(ctx context.Context, blockHash types.Hash) (*types.LocalizedBlockTrace, error) {
	return GetCfxClientFromContext(ctx).GetBlockTraces(blockHash)
}

func (api *traceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	return GetCfxClientFromContext(ctx).FilterTraces(filter)
}

func (api *traceAPI) Transaction(ctx context.Context, txHash types.Hash) ([]types.LocalizedTrace, error) {
	return GetCfxClientFromContext(ctx).GetTransactionTraces(txHash)
}
