package rpc

import (
	"context"

	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/web3go/types"
)

// ethTraceAPI provides evm space trace RPC proxy API.
type ethTraceAPI struct {
	stateHandler *handler.EthStateHandler
}

func (api *ethTraceAPI) Call(
	ctx context.Context,
	request types.CallRequest,
	options types.TraceOptions,
	blockNumOrHash *types.BlockNumberOrHash,
) (types.TraceResults, error) {
	logTraceRPCUnavailable("trace_call", map[string]any{"request": request, "options": options, "blockNumOrHash": blockNumOrHash})
	var result types.TraceResults
	return result, errTraceRPCUnavailable
}

func (api *ethTraceAPI) Block(ctx context.Context, blockNumOrHash types.BlockNumberOrHash) (cacheTypes.Lazy[[]types.LocalizedTrace], error) {
	logTraceRPCUnavailable("trace_block", map[string]any{"blockNumOrHash": blockNumOrHash})
	return cacheTypes.Lazy[[]types.LocalizedTrace]{}, errTraceRPCUnavailable
}

func (api *ethTraceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	logTraceRPCUnavailable("trace_filter", map[string]any{"filter": filter})
	return nil, errTraceRPCUnavailable
}

func (api *ethTraceAPI) Transaction(ctx context.Context, txHash common.Hash) (cacheTypes.Lazy[[]types.LocalizedTrace], error) {
	logTraceRPCUnavailable("trace_transaction", map[string]any{"txHash": txHash})
	return cacheTypes.Lazy[[]types.LocalizedTrace]{}, errTraceRPCUnavailable
}

func (api *ethTraceAPI) Get(ctx context.Context, txHash common.Hash, indexes []hexutil.Uint) (cacheTypes.Lazy[*types.LocalizedTrace], error) {
	logTraceRPCUnavailable("trace_get", map[string]any{"txHash": txHash, "indexes": indexes})
	return cacheTypes.Lazy[*types.LocalizedTrace]{}, errTraceRPCUnavailable
}

func (api *ethTraceAPI) BlockSetAuth(ctx context.Context, blockNumber types.BlockNumberOrHash) ([]types.LocalizedSetAuthTrace, error) {
	logTraceRPCUnavailable("trace_blockSetAuth", map[string]any{"blockNumber": blockNumber})
	return nil, errTraceRPCUnavailable
}
