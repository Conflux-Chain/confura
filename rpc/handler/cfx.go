package handler

import (
	"context"
	"errors"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
)

// CfxHandler interface delegated to handle cfx rpc request
type CfxHandler interface {
	GetBlockByHash(ctx context.Context, blockHash types.Hash, includeTxs bool) (interface{}, error)
	GetBlockByEpochNumber(ctx context.Context, epoch *types.Epoch, includeTxs bool) (interface{}, error)
	GetLogs(ctx context.Context, filter store.LogFilter) ([]types.Log, error)
	GetTransactionByHash(ctx context.Context, txHash types.Hash) (*types.Transaction, error)
	GetBlocksByEpoch(ctx context.Context, epoch *types.Epoch) ([]types.Hash, error)
	GetTransactionReceipt(ctx context.Context, txHash types.Hash) (*types.TransactionReceipt, error)
	GetBlockByBlockNumber(ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool) (block interface{}, err error)
}

// CfxStoreHandler implements cfxHandler interface to accelerate rpc request handling by loading epoch data from store
type CfxStoreHandler struct {
	store store.Store
	next  CfxHandler
}

func NewCfxStoreHandler(store store.Store, next CfxHandler) *CfxStoreHandler {
	return &CfxStoreHandler{store: store, next: next}
}

func (h *CfxStoreHandler) GetBlockByHash(ctx context.Context, blockHash types.Hash, includeTxs bool) (block interface{}, err error) {
	var sblock *store.Block
	var sblocksum *store.BlockSummary

	if includeTxs {
		sblock, err = h.store.GetBlockByHash(blockHash)
	} else {
		sblocksum, err = h.store.GetBlockSummaryByHash(blockHash)
	}

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlockByHash(ctx, blockHash, includeTxs)
	}

	if sblock != nil {
		return sblock.CfxBlock, nil
	}

	if sblocksum != nil {
		return sblocksum.CfxBlockSummary, nil
	}

	return
}

func (h *CfxStoreHandler) GetBlockByEpochNumber(ctx context.Context, epoch *types.Epoch, includeTxs bool) (block interface{}, err error) {
	epBigInt, ok := epoch.ToInt()
	if !ok {
		err = store.ErrUnsupported
		return
	}

	var sblock *store.Block
	var sblocksum *store.BlockSummary

	epochNo := epBigInt.Uint64()

	if includeTxs {
		sblock, err = h.store.GetBlockByEpoch(epochNo)
	} else {
		sblocksum, err = h.store.GetBlockSummaryByEpoch(epochNo)
	}

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlockByEpochNumber(ctx, epoch, includeTxs)
	}

	if sblock != nil {
		return sblock.CfxBlock, nil
	}

	if sblocksum != nil {
		return sblocksum.CfxBlockSummary, nil
	}

	return
}

func (h *CfxStoreHandler) GetLogs(ctx context.Context, filter store.LogFilter) (logs []types.Log, err error) {
	slogs, err := h.store.GetLogs(filter)
	if err == nil {
		logs = make([]types.Log, len(slogs))
		for i := 0; i < len(slogs); i++ {
			logs[i] = *slogs[i].CfxLog
		}

		return logs, nil
	}

	switch {
	case h.store.IsRecordNotFound(err):
	case errors.Is(err, store.ErrUnsupported):
	case errors.Is(err, store.ErrAlreadyPruned):
	case errors.Is(err, store.ErrGetLogsTooMany):
	case errors.Is(err, store.ErrGetLogsTimeout):
	default: // must be something wrong with the store
		logrus.WithError(err).Error("cfxStoreHandler failed to get logs from store")
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetLogs(ctx, filter)
	}

	return
}

func (h *CfxStoreHandler) GetTransactionByHash(ctx context.Context, txHash types.Hash) (*types.Transaction, error) {
	stx, err := h.store.GetTransaction(txHash)
	if err == nil {
		return stx.CfxTransaction, nil
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetTransactionByHash(ctx, txHash)
	}

	return nil, err
}

func (h *CfxStoreHandler) GetBlocksByEpoch(ctx context.Context, epoch *types.Epoch) (blockHashes []types.Hash, err error) {
	epBigInt, ok := epoch.ToInt()
	if !ok {
		err = store.ErrUnsupported
		return
	}

	epochNo := epBigInt.Uint64()
	if blockHashes, err = h.store.GetBlocksByEpoch(epochNo); err == nil {
		return
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlocksByEpoch(ctx, epoch)
	}

	return
}

func (h *CfxStoreHandler) GetBlockByBlockNumber(ctx context.Context, blockNumer hexutil.Uint64, includeTxs bool) (
	block interface{}, err error,
) {
	var sblock *store.Block
	var sblocksum *store.BlockSummary

	if includeTxs {
		sblock, err = h.store.GetBlockByBlockNumber(uint64(blockNumer))
	} else {
		sblocksum, err = h.store.GetBlockSummaryByBlockNumber(uint64(blockNumer))
	}

	if err != nil && !util.IsInterfaceValNil(h.next) {
		return h.next.GetBlockByBlockNumber(ctx, blockNumer, includeTxs)
	}

	if sblock != nil {
		return sblock.CfxBlock, nil
	}

	if sblocksum != nil {
		return sblocksum.CfxBlockSummary, nil
	}

	return
}

func (h *CfxStoreHandler) GetTransactionReceipt(ctx context.Context, txHash types.Hash) (*types.TransactionReceipt, error) {
	stxRcpt, err := h.store.GetReceipt(txHash)
	if err == nil {
		return stxRcpt.CfxReceipt, nil
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetTransactionReceipt(ctx, txHash)
	}

	return nil, err
}
