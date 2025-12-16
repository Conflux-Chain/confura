package handler

import (
	"context"
	"errors"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// CfxStoreHandler RPC handler to get block/txn/receipt data from store.
type CfxStoreHandler struct {
	sname string // store name
	store *mysql.CfxStore
}

func NewCfxStoreHandler(sname string, store *mysql.CfxStore) *CfxStoreHandler {
	return &CfxStoreHandler{sname: sname, store: store}
}

func (h *CfxStoreHandler) GetBlockByHash(
	ctx context.Context, blockHash types.Hash, includeTxs bool,
) (block interface{}, err error) {
	if includeTxs {
		block, err = h.store.GetBlockByHash(ctx, blockHash)
	} else {
		block, err = h.store.GetBlockSummaryByHash(ctx, blockHash)
	}

	h.collectHitStats("cfx_getBlockByHash", err)
	return
}

func (h *CfxStoreHandler) GetBlockByEpochNumber(
	ctx context.Context, epoch *types.Epoch, includeTxs bool,
) (block interface{}, err error) {
	epBigInt, ok := epoch.ToInt()
	if !ok { // epoch tags are not supported
		return nil, store.ErrUnsupported
	}

	epochNo := epBigInt.Uint64()
	if includeTxs {
		block, err = h.store.GetBlockByEpoch(ctx, epochNo)
	} else {
		block, err = h.store.GetBlockSummaryByEpoch(ctx, epochNo)
	}

	h.collectHitStats("cfx_getBlockByEpochNumber", err)
	return
}

func (h *CfxStoreHandler) GetBlocksByEpoch(ctx context.Context, epoch *types.Epoch) ([]types.Hash, error) {
	epBigInt, ok := epoch.ToInt()
	if !ok { // epoch tags are not supported
		return nil, store.ErrUnsupported
	}

	blockHashes, err := h.store.GetBlocksByEpoch(ctx, epBigInt.Uint64())
	h.collectHitStats("cfx_getBlocksByEpoch", err)
	return blockHashes, err
}

func (h *CfxStoreHandler) GetBlockByBlockNumber(
	ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool,
) (block interface{}, err error) {
	if includeTxs {
		block, err = h.store.GetBlockByBlockNumber(ctx, uint64(blockNumer))
	} else {
		block, err = h.store.GetBlockSummaryByBlockNumber(ctx, uint64(blockNumer))
	}

	h.collectHitStats("cfx_getBlockByBlockNumber", err)
	return
}

func (h *CfxStoreHandler) GetTransactionByHash(ctx context.Context, txHash types.Hash) (*types.Transaction, error) {
	txn, err := h.store.GetTransaction(ctx, txHash)
	h.collectHitStats("cfx_getTransactionByHash", err)
	return txn, err
}

func (h *CfxStoreHandler) GetTransactionReceipt(ctx context.Context, txHash types.Hash) (*types.TransactionReceipt, error) {
	rcpt, err := h.store.GetReceipt(ctx, txHash)
	h.collectHitStats("cfx_getTransactionReceipt", err)
	return rcpt, err
}

func (h *CfxStoreHandler) collectHitStats(method string, err error) {
	if !errors.Is(err, store.ErrUnsupported) { // ignore unsupported samples
		metrics.Registry.RPC.StoreHit(method, h.sname).Mark(err == nil)
	}
}
