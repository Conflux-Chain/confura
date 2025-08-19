package handler

import (
	"context"
	"math/big"

	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
)

// EthStateHandler handles evm space state RPC method by redirecting requests to another
// full state node if state is not available on normal full node.
type EthStateHandler struct {
	cp *node.EthClientProvider
}

func NewEthStateHandler(cp *node.EthClientProvider) *EthStateHandler {
	return &EthStateHandler{cp: cp}
}

func (h *EthStateHandler) Balance(
	ctx context.Context,
	w3c *node.Web3goClient,
	addr common.Address,
	block *types.BlockNumberOrHash,
) (*big.Int, error) {
	bal, err, usefs := tryResolveState(h, ctx, w3c, func(w3c *node.Web3goClient) (*big.Int, error) {
		return w3c.Eth.Balance(addr, block)
	})

	metrics.Registry.RPC.Percentage("eth_getBalance", "fullState").Mark(usefs)
	return bal, err
}

func (h *EthStateHandler) TransactionCount(
	ctx context.Context,
	w3c *node.Web3goClient,
	addr common.Address,
	blockNum *types.BlockNumberOrHash,
) (*big.Int, error) {
	txnCnt, err, usefs := tryResolveState(h, ctx, w3c, func(w3c *node.Web3goClient) (*big.Int, error) {
		return w3c.Eth.TransactionCount(addr, blockNum)
	})

	metrics.Registry.RPC.Percentage("eth_getTransactionCount", "fullState").Mark(usefs)
	return txnCnt, err
}

func (h *EthStateHandler) StorageAt(
	ctx context.Context,
	w3c *node.Web3goClient,
	addr common.Address,
	location *big.Int,
	block *types.BlockNumberOrHash,
) (common.Hash, error) {
	storage, err, usefs := tryResolveState(h, ctx, w3c, func(w3c *node.Web3goClient) (common.Hash, error) {
		return w3c.Eth.StorageAt(addr, location, block)
	})

	metrics.Registry.RPC.Percentage("eth_getStorageAt", "fullState").Mark(usefs)
	return storage, err
}

func (h *EthStateHandler) CodeAt(
	ctx context.Context,
	w3c *node.Web3goClient,
	addr common.Address,
	blockNum *types.BlockNumberOrHash,
) ([]byte, error) {
	code, err, usefs := tryResolveState(h, ctx, w3c, func(w3c *node.Web3goClient) ([]byte, error) {
		return w3c.Eth.CodeAt(addr, blockNum)
	})

	metrics.Registry.RPC.Percentage("eth_getCode", "fullState").Mark(usefs)
	return code, err
}

func (h *EthStateHandler) Call(
	ctx context.Context,
	w3c *node.Web3goClient,
	callRequest types.CallRequest,
	blockNum *types.BlockNumberOrHash,
	overrides *types.StateOverride,
	blockOverrides *types.BlockOverrides,
) ([]byte, error) {
	result, err, usefs := tryResolveState(h, ctx, w3c, func(w3c *node.Web3goClient) ([]byte, error) {
		return w3c.Eth.Call(callRequest, blockNum, overrides, blockOverrides)
	})

	metrics.Registry.RPC.Percentage("eth_call", "fullState").Mark(usefs)
	return result, err
}

func (h *EthStateHandler) EstimateGas(
	ctx context.Context,
	w3c *node.Web3goClient,
	callRequest types.CallRequest,
	blockNum *types.BlockNumberOrHash,
	overrides *types.StateOverride,
	blockOverrides *types.BlockOverrides,
) (*big.Int, error) {
	est, err, usefs := tryResolveState(h, ctx, w3c, func(w3c *node.Web3goClient) (*big.Int, error) {
		return w3c.Eth.EstimateGas(callRequest, blockNum, overrides, blockOverrides)
	})

	metrics.Registry.RPC.Percentage("eth_estimateGas", "fullState").Mark(usefs)
	return est, err
}

func (h *EthStateHandler) DebugTraceTransaction(
	ctx context.Context,
	w3c *node.Web3goClient,
	txnHash common.Hash,
	opts ...*types.GethDebugTracingOptions,
) (*types.GethTrace, error) {
	result, err, usefs := tryResolveState(h, ctx, w3c, func(w3c *node.Web3goClient) (*types.GethTrace, error) {
		return w3c.Debug.TraceTransaction(txnHash, opts...)
	})

	metrics.Registry.RPC.Percentage("debug_traceTransaction", "fullState").Mark(usefs)
	return result, err
}

func (h *EthStateHandler) DebugTraceBlockByHash(
	ctx context.Context,
	w3c *node.Web3goClient,
	blockHash common.Hash,
	opts ...*types.GethDebugTracingOptions,
) ([]*types.GethTraceResult, error) {
	result, err, usefs := tryResolveState(h, ctx, w3c, func(w3c *node.Web3goClient) ([]*types.GethTraceResult, error) {
		return w3c.Debug.TraceBlockByHash(blockHash, opts...)
	})

	metrics.Registry.RPC.Percentage("debug_traceBlockByHash", "fullState").Mark(usefs)
	return result, err
}

func (h *EthStateHandler) DebugTraceBlockByNumber(
	ctx context.Context,
	w3c *node.Web3goClient,
	blockNumber types.BlockNumber,
	opts ...*types.GethDebugTracingOptions,
) ([]*types.GethTraceResult, error) {
	result, err, usefs := tryResolveState(h, ctx, w3c, func(w3c *node.Web3goClient) ([]*types.GethTraceResult, error) {
		return w3c.Debug.TraceBlockByNumber(blockNumber, opts...)
	})

	metrics.Registry.RPC.Percentage("debug_traceBlockByNumber", "fullState").Mark(usefs)
	return result, err
}

func (h *EthStateHandler) DebugTraceCall(
	ctx context.Context,
	w3c *node.Web3goClient,
	request types.CallRequest,
	blockNumber *types.BlockNumber,
	opts ...*types.GethDebugTracingOptions,
) (*types.GethTrace, error) {
	result, err, usefs := tryResolveState(h, ctx, w3c, func(w3c *node.Web3goClient) (*types.GethTrace, error) {
		return w3c.Debug.TraceCall(request, blockNumber, opts...)
	})

	metrics.Registry.RPC.Percentage("debug_traceCall", "fullState").Mark(usefs)
	return result, err
}

func (h *EthStateHandler) TraceBlock(
	ctx context.Context,
	w3c *node.Web3goClient,
	blockNumOrHash types.BlockNumberOrHash,
) ([]types.LocalizedTrace, error) {
	lazyTrace, err := h.LazyTraceBlock(ctx, w3c, blockNumOrHash)
	if err != nil {
		return nil, err
	}
	return lazyTrace.Load()
}

func (h *EthStateHandler) LazyTraceBlock(
	ctx context.Context,
	w3c *node.Web3goClient,
	blockNumOrHash types.BlockNumberOrHash,
) (res cacheTypes.Lazy[[]types.LocalizedTrace], err error) {
	result, err, usefs := tryResolveState(h, ctx, w3c,
		func(w3c *node.Web3goClient) (cacheTypes.Lazy[[]types.LocalizedTrace], error) {
			return w3c.Trace.LazyBlocks(blockNumOrHash)
		},
	)

	metrics.Registry.RPC.Percentage("trace_block", "fullState").Mark(usefs)
	return result, err
}

func (h *EthStateHandler) TraceTransaction(
	ctx context.Context,
	w3c *node.Web3goClient,
	txHash common.Hash,
) ([]types.LocalizedTrace, error) {
	res, err := h.LazyTraceTransaction(ctx, w3c, txHash)
	if err != nil {
		return nil, err
	}
	return res.Load()
}

func (h *EthStateHandler) LazyTraceTransaction(
	ctx context.Context,
	w3c *node.Web3goClient,
	txHash common.Hash,
) (res cacheTypes.Lazy[[]types.LocalizedTrace], err error) {
	result, err, usefs := tryResolveState(h, ctx, w3c,
		func(w3c *node.Web3goClient) (cacheTypes.Lazy[[]types.LocalizedTrace], error) {
			return w3c.Trace.LazyTransactions(txHash)
		},
	)

	metrics.Registry.RPC.Percentage("trace_transaction", "fullState").Mark(usefs)
	return result, err
}

func (h *EthStateHandler) TraceGet(
	ctx context.Context,
	w3c *node.Web3goClient,
	txHash common.Hash,
	indexes []uint,
) (*types.LocalizedTrace, error) {
	result, err, usefs := tryResolveState(h, ctx, w3c,
		func(w3c *node.Web3goClient) (*types.LocalizedTrace, error) {
			return w3c.Trace.Trace(txHash, indexes)
		},
	)

	metrics.Registry.RPC.Percentage("trace_get", "fullState").Mark(usefs)
	return result, err
}

func (h *EthStateHandler) TraceFilter(
	ctx context.Context,
	w3c *node.Web3goClient,
	filter types.TraceFilter,
) ([]types.LocalizedTrace, error) {
	result, err, usefs := tryResolveState(h, ctx, w3c, func(w3c *node.Web3goClient) ([]types.LocalizedTrace, error) {
		return w3c.Trace.Filter(filter)
	})

	metrics.Registry.RPC.Percentage("trace_filter", "fullState").Mark(usefs)
	return result, err
}

func (h *EthStateHandler) TraceBlockSetAuth(
	ctx context.Context,
	w3c *node.Web3goClient,
	blockNumber types.BlockNumberOrHash,
) ([]types.LocalizedSetAuthTrace, error) {
	result, err, usefs := tryResolveState(h, ctx, w3c,
		func(w3c *node.Web3goClient) ([]types.LocalizedSetAuthTrace, error) {
			return w3c.Trace.BlockSetAuthTraces(blockNumber)
		},
	)

	metrics.Registry.RPC.Percentage("trace_blockSetAuth", "fullState").Mark(usefs)
	return result, err
}

func tryResolveState[T any](
	h *EthStateHandler,
	ctx context.Context,
	initW3c *node.Web3goClient,
	clientFunc func(w3c *node.Web3goClient) (T, error),
) (T, error, bool) {
	result, err := clientFunc(initW3c)
	if err == nil || !isStateNotAvailable(err) {
		return result, err, false
	}

	fsW3c, cperr := h.cp.GetClientByIP(ctx, node.GroupEthFullState)
	if cperr == nil {
		result, err = clientFunc(fsW3c)
	}

	return result, err, true
}
