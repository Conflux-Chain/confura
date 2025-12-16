package handler

import (
	"context"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/ethereum/go-ethereum/common"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// EthStoreHandler RPC handler to get block/txn/receipt data from store.
type EthStoreHandler struct {
	store *mysql.EthStore
}

func NewEthStoreHandler(store *mysql.EthStore) *EthStoreHandler {
	return &EthStoreHandler{store: store}
}

func (h *EthStoreHandler) GetBlockByHash(
	ctx context.Context, blockHash common.Hash, includeTxs bool,
) (*web3Types.Block, error) {
	block, err := h.store.GetBlockByHash(blockHash, includeTxs)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"blockHash": blockHash, "includeTxs": includeTxs,
		}).WithError(err).Debug("ETH handler failed to handle GetBlockByHash")
	}

	return block, err
}

func (h *EthStoreHandler) GetBlockByNumber(
	ctx context.Context, blockNum web3Types.BlockNumber, includeTxs bool,
) (*web3Types.Block, error) {
	if blockNum <= 0 { // block tags are not supported
		return nil, store.ErrUnsupported
	}

	block, err := h.store.GetBlockByNumber(blockNum, includeTxs)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"blockNum": blockNum, "includeTxs": includeTxs,
		}).WithError(err).Debug("ETH handler failed to handle GetBlockByNumber")
	}

	return block, err
}

func (h *EthStoreHandler) GetLogs(ctx context.Context, filter store.LogFilter) (logs []web3Types.Log, err error) {
	slogs, err := h.store.GetLogs(ctx, filter)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"filter": filter,
		}).WithError(err).Debug("ethStoreHandler failed to get logs from store")
		return nil, err
	}

	logs = make([]web3Types.Log, len(slogs))
	for i := 0; i < len(slogs); i++ {
		log, err := slogs[i].ToEthLog()
		if err != nil {
			return nil, errors.WithMessage(err, "failed to convert to eth log")
		}
		logs[i] = *log
	}
	return logs, nil
}

func (h *EthStoreHandler) GetTransactionByHash(ctx context.Context, txHash common.Hash) (*web3Types.TransactionDetail, error) {
	txn, err := h.store.GetTransactionByHash(txHash)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"txHash": txHash,
		}).WithError(err).Debug("ETH handler failed to handle GetTransactionByHash")
	}

	return txn, err
}

func (h *EthStoreHandler) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*web3Types.Receipt, error) {
	rcpt, err := h.store.GetTransactionReceipt(txHash)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"txHash": txHash,
		}).WithError(err).Debug("ETH handler failed to handle GetTransactionReceipt")
	}

	return rcpt, err
}
