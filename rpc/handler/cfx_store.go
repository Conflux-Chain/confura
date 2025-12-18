package handler

import (
	"context"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
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

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"blockHash":  blockHash,
			"includeTxs": includeTxs,
		}).WithError(err).Debug("CFX handler failed to handle GetBlockByHash")
	}
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

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"epoch":      epoch,
			"includeTxs": includeTxs,
		}).WithError(err).Debug("CFX handler failed to handle GetBlockByEpochNumber")
	}
	return
}

func (h *CfxStoreHandler) GetBlocksByEpoch(ctx context.Context, epoch *types.Epoch) ([]types.Hash, error) {
	epBigInt, ok := epoch.ToInt()
	if !ok { // epoch tags are not supported
		return nil, store.ErrUnsupported
	}

	blockHashes, err := h.store.GetBlocksByEpoch(ctx, epBigInt.Uint64())
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"epoch": epoch,
		}).WithError(err).Debug("CFX handler failed to handle GetBlocksByEpoch")
	}

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

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"blockNumer": blockNumer,
			"includeTxs": includeTxs,
		}).WithError(err).Debug("CFX handler failed to handle GetBlockByBlockNumber")
	}
	return
}

func (h *CfxStoreHandler) GetTransactionByHash(ctx context.Context, txHash types.Hash) (*types.Transaction, error) {
	txn, err := h.store.GetTransaction(ctx, txHash)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"txHash": txHash,
		}).WithError(err).Debug("CFX handler failed to handle GetTransactionByHash")
	}

	return txn, err
}

func (h *CfxStoreHandler) GetTransactionReceipt(ctx context.Context, txHash types.Hash) (*types.TransactionReceipt, error) {
	rcpt, err := h.store.GetReceipt(ctx, txHash)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"txHash": txHash,
		}).WithError(err).Debug("CFX handler failed to handle GetTransactionReceipt")
	}

	return rcpt, err
}
