package rpc

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
)

// ethTraceAPI provides evm space trace RPC proxy API.
type ethTraceAPI struct{}

func (api *ethTraceAPI) Block(ctx context.Context, blockNumOrHash types.BlockNumberOrHash) ([]types.LocalizedTrace, error) {
	return GetEthClientFromContext(ctx).Trace.Blocks(blockNumOrHash)
}

func (api *ethTraceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	return GetEthClientFromContext(ctx).Trace.Filter(filter)
}

func (api *ethTraceAPI) Transaction(ctx context.Context, txHash common.Hash) ([]types.LocalizedTrace, error) {
	return GetEthClientFromContext(ctx).Trace.Transactions(txHash)
}
