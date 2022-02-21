package rpc

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
)

type ethTraceAPI struct {
	w3c *web3go.Client
}

func newEthTraceAPI(eth *web3go.Client) *ethTraceAPI {
	return &ethTraceAPI{w3c: eth}
}

func (api *ethTraceAPI) Block(ctx context.Context, blockNumOrHash types.BlockNumberOrHash) ([]types.LocalizedTrace, error) {
	return api.w3c.Trace.Blocks(blockNumOrHash)
}

func (api *ethTraceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	return api.w3c.Trace.Filter(filter)
}

func (api *ethTraceAPI) Transaction(ctx context.Context, txHash common.Hash) ([]types.LocalizedTrace, error) {
	return api.w3c.Trace.Transactions(txHash)
}
