package handler

import (
	"context"
	"errors"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/conflux-chain/conflux-infura/util/metrics"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var (
	_ CfxStoreHandler = (*CfxCommonStoreHandler)(nil)
)

// CfxStoreHandler interface delegated to handle Conflux RPC request by loading epoch data from store.
type CfxStoreHandler interface {
	GetBlockByHash(ctx context.Context, blockHash types.Hash, includeTxs bool) (interface{}, error)
	GetBlockByEpochNumber(ctx context.Context, epoch *types.Epoch, includeTxs bool) (interface{}, error)
	GetLogs(ctx context.Context, filter store.LogFilter) ([]types.Log, error)
	GetTransactionByHash(ctx context.Context, txHash types.Hash) (*types.Transaction, error)
	GetBlocksByEpoch(ctx context.Context, epoch *types.Epoch) ([]types.Hash, error)
	GetTransactionReceipt(ctx context.Context, txHash types.Hash) (*types.TransactionReceipt, error)
	GetBlockByBlockNumber(ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool) (block interface{}, err error)
}

type CfxCommonStoreHandler struct {
	sname string // store name
	store store.Store

	next CfxStoreHandler
}

func NewCfxCommonStoreHandler(sname string, store store.Store, next CfxStoreHandler) *CfxCommonStoreHandler {
	return &CfxCommonStoreHandler{
		sname: sname, store: store, next: next,
	}
}

func (h *CfxCommonStoreHandler) GetBlockByHash(
	ctx context.Context, blockHash types.Hash, includeTxs bool) (block interface{}, err error) {
	if store.StoreConfig().IsChainBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	if includeTxs {
		block, err = h.store.GetBlockByHash(blockHash)
	} else {
		block, err = h.store.GetBlockSummaryByHash(blockHash)
	}

	h.collectHitStats("cfx_getBlockByHash", err)

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlockByHash(ctx, blockHash, includeTxs)
	}

	return
}

func (h *CfxCommonStoreHandler) GetBlockByEpochNumber(ctx context.Context, epoch *types.Epoch, includeTxs bool) (block interface{}, err error) {
	epBigInt, ok := epoch.ToInt()
	if store.StoreConfig().IsChainBlockDisabled() || !ok {
		return nil, store.ErrUnsupported
	}

	epochNo := epBigInt.Uint64()

	if includeTxs {
		block, err = h.store.GetBlockByEpoch(epochNo)
	} else {
		block, err = h.store.GetBlockSummaryByEpoch(epochNo)
	}

	h.collectHitStats("cfx_getBlockByEpochNumber", err)

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlockByEpochNumber(ctx, epoch, includeTxs)
	}

	return
}

func (h *CfxCommonStoreHandler) GetTransactionByHash(ctx context.Context, txHash types.Hash) (txn *types.Transaction, err error) {
	if store.StoreConfig().IsChainTxnDisabled() {
		return nil, store.ErrUnsupported
	}

	stxn, err := h.store.GetTransaction(txHash)
	if err == nil {
		txn = stxn.CfxTransaction
	}

	h.collectHitStats("cfx_getTransactionByHash", err)

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetTransactionByHash(ctx, txHash)
	}

	return
}

func (h *CfxCommonStoreHandler) GetBlocksByEpoch(ctx context.Context, epoch *types.Epoch) (blockHashes []types.Hash, err error) {
	epBigInt, ok := epoch.ToInt()
	if store.StoreConfig().IsChainBlockDisabled() || !ok {
		return nil, store.ErrUnsupported
	}

	epochNo := epBigInt.Uint64()
	blockHashes, err = h.store.GetBlocksByEpoch(epochNo)

	h.collectHitStats("cfx_getBlocksByEpoch", err)

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlocksByEpoch(ctx, epoch)
	}

	return
}

func (h *CfxCommonStoreHandler) GetBlockByBlockNumber(
	ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool,
) (block interface{}, err error) {
	if store.StoreConfig().IsChainBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	if includeTxs {
		block, err = h.store.GetBlockByBlockNumber(uint64(blockNumer))
	} else {
		block, err = h.store.GetBlockSummaryByBlockNumber(uint64(blockNumer))
	}

	h.collectHitStats("cfx_getBlockByBlockNumber", err)

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlockByBlockNumber(ctx, blockNumer, includeTxs)
	}

	return
}

func (h *CfxCommonStoreHandler) GetTransactionReceipt(ctx context.Context, txHash types.Hash) (rcpt *types.TransactionReceipt, err error) {
	if store.StoreConfig().IsChainReceiptDisabled() {
		return nil, store.ErrUnsupported
	}

	stxRcpt, err := h.store.GetReceipt(txHash)
	if err == nil {
		rcpt = stxRcpt.CfxReceipt
	}

	h.collectHitStats("cfx_getTransactionReceipt", err)

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetTransactionReceipt(ctx, txHash)
	}

	return
}

func (h *CfxCommonStoreHandler) GetLogs(ctx context.Context, filter store.LogFilter) (logs []types.Log, err error) {
	if store.StoreConfig().IsChainLogDisabled() {
		return nil, store.ErrUnsupported
	}

	slogs, err := h.store.GetLogs(filter)
	if err == nil {
		logs = make([]types.Log, len(slogs))

		for i := 0; i < len(slogs); i++ {
			logs[i] = *slogs[i].CfxLog
		}
	}

	h.collectHitStats("cfx_getLogs", err)

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetLogs(ctx, filter)
	}

	return
}

func (h *CfxCommonStoreHandler) collectHitStats(method string, err error) {
	if !errors.Is(err, store.ErrUnsupported) {
		return
	}

	hitStore := (err == nil) || errors.Is(err, store.ErrGetLogsResultSetTooLarge)

	metrics.Registry.RPC.StoreHit(method, h.sname).Mark(hitStore)
}
