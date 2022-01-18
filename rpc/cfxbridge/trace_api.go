package cfxbridge

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

// TODO add implementation when fullnode supports.
type TraceAPI struct {
}

func NewTraceAPI() *TraceAPI {
	return &TraceAPI{}
}

func (api *TraceAPI) Block(ctx context.Context, blockHash types.Hash) (*types.LocalizedBlockTrace, error) {
	return &types.LocalizedBlockTrace{
		TransactionTraces: []types.LocalizedTransactionTrace{},
		BlockHash:         blockHash,
	}, nil
}

func (api *TraceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	return []types.LocalizedTrace{}, nil
}

func (api *TraceAPI) Transaction(ctx context.Context, txHash types.Hash) ([]types.LocalizedTrace, error) {
	return []types.LocalizedTrace{}, nil
}
