package handler

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/scroll-tech/rpc-gateway/rpc/cfxbridge"
	"github.com/scroll-tech/rpc-gateway/rpc/ethbridge"
	"github.com/scroll-tech/rpc-gateway/store"
	"github.com/scroll-tech/rpc-gateway/util"
	"github.com/sirupsen/logrus"
)

// EthStoreHandler RPC handler to get block/txn/receipt data from store.
type EthStoreHandler struct {
	store store.Readable
	next  *EthStoreHandler
}

func NewEthStoreHandler(store store.Readable, next *EthStoreHandler) *EthStoreHandler {
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
		sblock, err = h.store.GetBlockByHash(ctx, cfxBlockHash)
	} else {
		sblocksum, err = h.store.GetBlockSummaryByHash(ctx, cfxBlockHash)
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
		sblock, err = h.store.GetBlockByBlockNumber(ctx, uint64(*blockNum))
	} else {
		sblocksum, err = h.store.GetBlockSummaryByBlockNumber(ctx, uint64(*blockNum))
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
	if store.EthStoreConfig().IsChainLogDisabled() {
		return nil, store.ErrUnsupported
	}

	slogs, err := h.store.GetLogs(ctx, filter)
	if err == nil {
		logs = make([]web3Types.Log, len(slogs))
		for i := 0; i < len(slogs); i++ {
			logs[i] = *ethbridge.ConvertLog(slogs[i].ToCfxLog())
		}

		return logs, nil
	}

	logrus.WithError(err).Info("ethStoreHandler failed to get logs from store")

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetLogs(ctx, filter)
	}

	return
}

func (h *EthStoreHandler) GetTransactionByHash(ctx context.Context, txHash common.Hash) (*web3Types.TransactionDetail, error) {
	cfxTxHash := cfxbridge.ConvertHash(txHash)

	stx, err := h.store.GetTransaction(ctx, cfxTxHash)
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

	stxRcpt, err := h.store.GetReceipt(ctx, cfxTxHash)
	if err == nil {
		return ethbridge.ConvertReceipt(stxRcpt.CfxReceipt, stxRcpt.Extra), nil
	}

	if !util.IsInterfaceValNil(h.next) {
		return h.next.GetTransactionReceipt(ctx, txHash)
	}

	return nil, err
}
