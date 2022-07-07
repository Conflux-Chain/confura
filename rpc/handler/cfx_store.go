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

// CfxStoreHandler RPC handler to get block/txn/receipt data from store.
type CfxStoreHandler struct {
	sname string // store name
	store store.Readable

	next *CfxStoreHandler
}

func NewCfxCommonStoreHandler(sname string, store store.Readable, next *CfxStoreHandler) *CfxStoreHandler {
	return &CfxStoreHandler{
		sname: sname, store: store, next: next,
	}
}

func (h *CfxStoreHandler) GetBlockByHash(
	ctx context.Context, blockHash types.Hash, includeTxs bool) (block interface{}, err error) {
	if store.StoreConfig().IsChainBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	if includeTxs {
		block, err = h.store.GetBlockByHash(ctx, blockHash)
	} else {
		block, err = h.store.GetBlockSummaryByHash(ctx, blockHash)
	}

	h.collectHitStats("cfx_getBlockByHash", err)

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlockByHash(ctx, blockHash, includeTxs)
	}

	return
}

func (h *CfxStoreHandler) GetBlockByEpochNumber(
	ctx context.Context, epoch *types.Epoch, includeTxs bool,
) (block interface{}, err error) {
	epBigInt, ok := epoch.ToInt()
	if store.StoreConfig().IsChainBlockDisabled() || !ok {
		return nil, store.ErrUnsupported
	}

	epochNo := epBigInt.Uint64()

	if includeTxs {
		block, err = h.store.GetBlockByEpoch(ctx, epochNo)
	} else {
		block, err = h.store.GetBlockSummaryByEpoch(ctx, epochNo)
	}

	h.collectHitStats("cfx_getBlockByEpochNumber", err)

	if err != nil && h.next != nil {
		return h.next.GetBlockByEpochNumber(ctx, epoch, includeTxs)
	}

	return
}

func (h *CfxStoreHandler) GetTransactionByHash(ctx context.Context, txHash types.Hash) (txn *types.Transaction, err error) {
	if store.StoreConfig().IsChainTxnDisabled() {
		return nil, store.ErrUnsupported
	}

	stxn, err := h.store.GetTransaction(ctx, txHash)
	if err == nil {
		txn = stxn.CfxTransaction
	}

	h.collectHitStats("cfx_getTransactionByHash", err)

	if err != nil && h.next != nil {
		return h.next.GetTransactionByHash(ctx, txHash)
	}

	return
}

func (h *CfxStoreHandler) GetBlocksByEpoch(
	ctx context.Context, epoch *types.Epoch,
) (blockHashes []types.Hash, err error) {
	epBigInt, ok := epoch.ToInt()
	if store.StoreConfig().IsChainBlockDisabled() || !ok {
		return nil, store.ErrUnsupported
	}

	epochNo := epBigInt.Uint64()
	blockHashes, err = h.store.GetBlocksByEpoch(ctx, epochNo)

	h.collectHitStats("cfx_getBlocksByEpoch", err)

	if err != nil && h.next != nil {
		return h.next.GetBlocksByEpoch(ctx, epoch)
	}

	return
}

func (h *CfxStoreHandler) GetBlockByBlockNumber(
	ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool,
) (block interface{}, err error) {
	if store.StoreConfig().IsChainBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	if includeTxs {
		block, err = h.store.GetBlockByBlockNumber(ctx, uint64(blockNumer))
	} else {
		block, err = h.store.GetBlockSummaryByBlockNumber(ctx, uint64(blockNumer))
	}

	h.collectHitStats("cfx_getBlockByBlockNumber", err)

	if err != nil && h.next != nil {
		return h.next.GetBlockByBlockNumber(ctx, blockNumer, includeTxs)
	}

	return
}

func (h *CfxStoreHandler) GetTransactionReceipt(
	ctx context.Context, txHash types.Hash,
) (rcpt *types.TransactionReceipt, err error) {
	if store.StoreConfig().IsChainReceiptDisabled() {
		return nil, store.ErrUnsupported
	}

	stxRcpt, err := h.store.GetReceipt(ctx, txHash)
	if err == nil {
		rcpt = stxRcpt.CfxReceipt
	}

	h.collectHitStats("cfx_getTransactionReceipt", err)

	if err != nil && h.next != nil {
		return h.next.GetTransactionReceipt(ctx, txHash)
	}

	return
}

func (h *CfxStoreHandler) collectHitStats(method string, err error) {
	if !errors.Is(err, store.ErrUnsupported) { // ignore unsupported samples
		metrics.Registry.RPC.StoreHit(method, h.sname).Mark(err == nil)
	}
}
