package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/node"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
)

type ethTraceAPI struct {
	provider *node.EthClientProvider
}

func newEthTraceAPI(provider *node.EthClientProvider) *ethTraceAPI {
	return &ethTraceAPI{provider: provider}
}

func (api *ethTraceAPI) Block(ctx context.Context, blockNumOrHash types.BlockNumberOrHash) ([]types.LocalizedTrace, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return w3c.Trace.Blocks(blockNumOrHash)
}

func (api *ethTraceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return w3c.Trace.Filter(filter)
}

func (api *ethTraceAPI) Transaction(ctx context.Context, txHash common.Hash) ([]types.LocalizedTrace, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return w3c.Trace.Transactions(txHash)
}
