package rpc

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/node"
)

type traceAPI struct {
	provider *node.CfxClientProvider
}

func newTraceAPI(provider *node.CfxClientProvider) *traceAPI {
	return &traceAPI{provider}
}

func (api *traceAPI) Block(ctx context.Context, blockHash types.Hash) (*types.LocalizedBlockTrace, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return cfx.GetBlockTraces(blockHash)
}

func (api *traceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return cfx.FilterTraces(filter)
}

func (api *traceAPI) Transaction(ctx context.Context, txHash types.Hash) ([]types.LocalizedTrace, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return cfx.GetTransactionTraces(txHash)
}
