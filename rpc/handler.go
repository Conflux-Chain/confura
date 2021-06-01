package rpc

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/sirupsen/logrus"
)

// cfxHandler interface delegated to handle cfx rpc request
type cfxHandler interface {
	GetBlockByHash(ctx context.Context, blockHash types.Hash, includeTxs bool) (interface{}, error)
	GetBlockByEpochNumber(ctx context.Context, epoch *types.Epoch, includeTxs bool) (interface{}, error)
	GetLogs(ctx context.Context, filter store.LogFilter) ([]types.Log, error)
	GetTransactionByHash(ctx context.Context, txHash types.Hash) (*types.Transaction, error)
	GetBlocksByEpoch(ctx context.Context, epoch *types.Epoch) ([]types.Hash, error)
	GetTransactionReceipt(ctx context.Context, txHash types.Hash) (*types.TransactionReceipt, error)
}

// CfxStoreHandler implements cfxHandler interface to accelerate rpc request handling by loading epoch data from store
type CfxStoreHandler struct {
	store store.Store
	next  cfxHandler
}

func NewCfxStoreHandler(store store.Store, next cfxHandler) *CfxStoreHandler {
	return &CfxStoreHandler{store: store, next: next}
}

func (h *CfxStoreHandler) GetBlockByHash(ctx context.Context, blockHash types.Hash, includeTxs bool) (block interface{}, err error) {
	if includeTxs {
		if block, err = h.store.GetBlockByHash(blockHash); err == nil {
			return
		}
	} else if block, err = h.store.GetBlockSummaryByHash(blockHash); err == nil {
		return
	}

	if h.next != nil {
		return h.next.GetBlockByHash(ctx, blockHash, includeTxs)
	}

	return
}

func (h *CfxStoreHandler) GetBlockByEpochNumber(ctx context.Context, epoch *types.Epoch, includeTxs bool) (block interface{}, err error) {
	epBigInt, ok := epoch.ToInt()
	if !ok {
		err = store.ErrUnsupported
		return
	}

	epochNo := epBigInt.Uint64()
	if includeTxs {
		if block, err = h.store.GetBlockByEpoch(epochNo); err == nil {
			return
		}
	} else if block, err = h.store.GetBlockByEpoch(epochNo); err == nil {
		return
	}

	if h.next != nil {
		return h.next.GetBlockByEpochNumber(ctx, epoch, includeTxs)
	}

	return
}

func (h *CfxStoreHandler) GetLogs(ctx context.Context, filter store.LogFilter) (logs []types.Log, err error) {
	logs, err = h.store.GetLogs(filter)
	if err == nil {
		return logs, nil
	}

	if !h.store.IsRecordNotFound(err) && err != store.ErrUnsupported { // must be something wrong with the store
		logrus.WithError(err).Error("cfxStoreHandler failed to get logs from store")
	}

	if h.next != nil {
		return h.next.GetLogs(ctx, filter)
	}

	return
}

func (h *CfxStoreHandler) GetTransactionByHash(ctx context.Context, txHash types.Hash) (tx *types.Transaction, err error) {
	if tx, err = h.store.GetTransaction(txHash); err == nil {
		return
	}

	if h.next != nil {
		return h.next.GetTransactionByHash(ctx, txHash)
	}

	return
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

	if h.next != nil {
		return h.next.GetBlocksByEpoch(ctx, epoch)
	}

	return
}

func (h *CfxStoreHandler) GetTransactionReceipt(ctx context.Context, txHash types.Hash) (txRcpt *types.TransactionReceipt, err error) {
	if txRcpt, err = h.store.GetReceipt(txHash); err == nil {
		return
	}

	if h.next != nil {
		return h.next.GetTransactionReceipt(ctx, txHash)
	}

	return
}
