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

func (api *ethTraceAPI) Block(ctx context.Context, blockNumOrHash types.BlockNumberOrHash) (cacheTypes.Lazy[[]types.LocalizedTrace], error) {
	w3c := GetEthClientFromContext(ctx)
	return api.stateHandler.LazyTraceBlock(ctx, w3c, blockNumOrHash)
}

func (api *ethTraceAPI) Filter(ctx context.Context, filter types.TraceFilter) ([]types.LocalizedTrace, error) {
	w3c := GetEthClientFromContext(ctx)
	return api.stateHandler.TraceFilter(ctx, w3c, filter)
}

func (api *ethTraceAPI) Transaction(ctx context.Context, txHash common.Hash) (cacheTypes.Lazy[[]types.LocalizedTrace], error) {
	w3c := GetEthClientFromContext(ctx)
	return api.stateHandler.LazyTraceTransaction(ctx, w3c, txHash)
}

func (api *ethTraceAPI) Get(ctx context.Context, txHash common.Hash, indexes []hexutil.Uint) (*types.LocalizedTrace, error) {
	idxs := make([]uint, 0, len(indexes))
	for _, v := range indexes {
		idxs = append(idxs, uint(v))
	}

	w3c := GetEthClientFromContext(ctx)
	return api.stateHandler.TraceGet(ctx, w3c, txHash, idxs)
}

func (api *ethTraceAPI) BlockSetAuth(ctx context.Context, blockNumber types.BlockNumberOrHash) ([]types.LocalizedSetAuthTrace, error) {
	w3c := GetEthClientFromContext(ctx)
	return api.stateHandler.TraceBlockSetAuth(ctx, w3c, blockNumber)
}
