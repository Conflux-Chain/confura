package handler

import (
	"context"

	"github.com/conflux-chain/conflux-infura/rpc/cfxbridge"
	"github.com/conflux-chain/conflux-infura/rpc/ethbridge"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// EthHandler interface delegated to handle eth rpc request
type EthHandler interface {
	GetBlockByHash(ctx context.Context, blockHash common.Hash, includeTxs bool) (*web3Types.Block, error)
	GetBlockByNumber(ctx context.Context, blockNum *web3Types.BlockNumber, includeTxs bool) (*web3Types.Block, error)
	GetLogs(ctx context.Context, filter store.LogFilter) ([]web3Types.Log, error)
	GetTransactionByHash(ctx context.Context, hash common.Hash) (*web3Types.Transaction, error)
	GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*web3Types.Receipt, error)
}

// EthStoreHandler implements ethHandler interface to accelerate rpc request handling by
// loading ETH blockchain data from store
type EthStoreHandler struct {
	store store.Store
	next  EthHandler
}

func NewEthStoreHandler(store store.Store, next EthHandler) *EthStoreHandler {
	return &EthStoreHandler{store: store, next: next}
}

func (h *EthStoreHandler) GetBlockByHash(ctx context.Context, blockHash common.Hash, includeTxs bool) (
	block *web3Types.Block, err error,
) {
	logger := logrus.WithFields(logrus.Fields{
		"blockHash": blockHash, "includeTxs": includeTxs,
	})

	var sblock *store.Block
	var sblocksum *store.BlockSummary

	cfxBlockHash := cfxbridge.ConvertHash(blockHash)

	if includeTxs {
		sblock, err = h.store.GetBlockByHash(cfxBlockHash)
	} else {
		sblocksum, err = h.store.GetBlockSummaryByHash(cfxBlockHash)
	}

	if err != nil {
		logger.WithError(err).Debug("ETH handler failed to handle GetBlockByHash")
		if !util.IsInterfaceValNil(h.next) {
			return h.next.GetBlockByHash(ctx, blockHash, includeTxs)
		}
	}

	if sblock != nil {
		return ethbridge.ConvertBlock(sblock.CfxBlock, sblock.Extra), nil
	}

	if sblocksum != nil {
		return ethbridge.ConvertBlockSummary(sblocksum.CfxBlockSummary, sblocksum.Extra), nil
	}

	return
}

func (h *EthStoreHandler) GetBlockByNumber(ctx context.Context, blockNum *web3Types.BlockNumber, includeTxs bool) (
	block *web3Types.Block, err error,
) {
	logger := logrus.WithFields(logrus.Fields{
		"blockNum": *blockNum, "includeTxs": includeTxs,
	})

	if *blockNum <= 0 {
		return nil, store.ErrUnsupported
	}

	var sblock *store.Block
	var sblocksum *store.BlockSummary

	if includeTxs {
		sblock, err = h.store.GetBlockByBlockNumber(uint64(*blockNum))
	} else {
		sblocksum, err = h.store.GetBlockSummaryByBlockNumber(uint64(*blockNum))
	}

	if err != nil {
		logger.WithError(err).Debug("ETH handler failed to handle GetBlockByNumber")

		if !util.IsInterfaceValNil(h.next) {
			return h.next.GetBlockByNumber(ctx, blockNum, includeTxs)
		}
	}

	if sblock != nil {
		return ethbridge.ConvertBlock(sblock.CfxBlock, sblock.Extra), nil
	}

	if sblocksum != nil {
		return ethbridge.ConvertBlockSummary(sblocksum.CfxBlockSummary, sblocksum.Extra), nil
	}

	return
}

func (h *EthStoreHandler) GetLogs(ctx context.Context, filter store.LogFilter) (logs []web3Types.Log, err error) {
	slogs, err := h.store.GetLogs(filter)
	if err == nil {
		logs = make([]web3Types.Log, len(slogs))
		for i := 0; i < len(slogs); i++ {
			logs[i] = *ethbridge.ConvertLog(slogs[i].CfxLog, slogs[i].Extra)
		}

		return logs, nil
	}

	switch {
	case h.store.IsRecordNotFound(err):
	case errors.Is(err, store.ErrUnsupported):
	case errors.Is(err, store.ErrAlreadyPruned):
	default: // must be something wrong with the store
		logrus.WithError(err).Error("ethStoreHandler failed to get logs from store")
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetLogs(ctx, filter)
	}

	return
}

func (h *EthStoreHandler) GetTransactionByHash(ctx context.Context, txHash common.Hash) (*web3Types.Transaction, error) {
	cfxTxHash := cfxbridge.ConvertHash(txHash)

	stx, err := h.store.GetTransaction(cfxTxHash)
	if err == nil {
		return ethbridge.ConvertTx(stx.CfxTransaction, stx.Extra), nil
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetTransactionByHash(ctx, txHash)
	}

	return nil, err
}

func (h *EthStoreHandler) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*web3Types.Receipt, error) {
	cfxTxHash := cfxbridge.ConvertHash(txHash)

	stxRcpt, err := h.store.GetReceipt(cfxTxHash)
	if err == nil {
		return ethbridge.ConvertReceipt(stxRcpt.CfxReceipt, stxRcpt.Extra), nil
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetTransactionReceipt(ctx, txHash)
	}

	return nil, err
}
