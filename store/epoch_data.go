package store

import (
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// EpochData wraps the blockchain data of an epoch.
type EpochData struct {
	Number   uint64         // epoch number
	Blocks   []*types.Block // blocks in order and the last one is pivot block
	Receipts map[types.Hash]*types.TransactionReceipt
}

// QueryEpochData queries blockchain data for the specified epoch number.
// TODO better to use batch API to return all if performance is low in case of high TPS.
func QueryEpochData(cfx sdk.ClientOperator, epochNumber uint64) (EpochData, error) {
	epoch := types.NewEpochNumberUint64(epochNumber)

	blockHashes, err := cfx.GetBlocksByEpoch(epoch)
	if err != nil {
		return EpochData{}, errors.WithMessagef(err, "Failed to get blocks by epoch %v", epochNumber)
	}

	blocks := make([]*types.Block, 0, len(blockHashes))

	for _, hash := range blockHashes {
		block, err := cfx.GetBlockByHash(hash)
		if err != nil {
			return EpochData{}, errors.WithMessagef(err, "Failed to get block by hash %v", hash)
		}

		blocks = append(blocks, block)
	}

	receipts := make(map[types.Hash]*types.TransactionReceipt)
	var logIndex uint64

	for _, block := range blocks {
		for _, tx := range block.Transactions {
			// skip unexecuted transaction, e.g.
			// 1) already executed in previous block
			// 2) never executed, e.g. nonce mismatch
			if tx.Status == nil || tx.BlockHash == nil {
				if logrus.IsLevelEnabled(logrus.DebugLevel) {
					logrus.WithFields(logrus.Fields{
						"epoch": epochNumber,
						"block": block.Hash.String(),
						"tx":    tx.Hash.String(),
					}).Debug("Transaction not executed in block")
				}

				continue
			}

			receipt, err := cfx.GetTransactionReceipt(tx.Hash)
			if err != nil {
				return EpochData{}, errors.WithMessagef(err, "Failed to get receipt by tx hash %v", tx.Hash)
			}

			if receipt == nil {
				logrus.WithFields(logrus.Fields{
					"epoch": epochNumber,
					"block": block.Hash.String(),
					"tx":    tx.Hash.String(),
				}).Error("Transaction receipt not found")

				continue
			}

			if uint64(*receipt.EpochNumber) != epochNumber {
				logrus.WithFields(logrus.Fields{
					"epoch": epochNumber,
					"block": block.Hash.String(),
					"tx":    tx.Hash.String(),
				}).Errorf("Receipt epoch number mismatch, value = %v", uint64(*receipt.EpochNumber))
			}

			if receipt.BlockHash.String() != block.Hash.String() {
				logrus.WithFields(logrus.Fields{
					"epoch": epochNumber,
					"block": block.Hash.String(),
					"tx":    tx.Hash.String(),
				}).Errorf("Receipt block hash mismatch, value = %v", receipt.BlockHash.String())
			}

			// TODO enhance full node RPC to return receipts by block hash
			var txLogIndex uint64
			logs := make([]types.Log, 0, len(receipt.Logs))
			for _, log := range receipt.Logs {
				log.BlockHash = &receipt.BlockHash
				log.EpochNumber = types.NewBigInt(uint64(*receipt.EpochNumber))
				log.TransactionHash = &receipt.TransactionHash
				log.TransactionIndex = types.NewBigInt(uint64(receipt.Index))
				log.LogIndex = types.NewBigInt(logIndex)
				log.TransactionLogIndex = types.NewBigInt(txLogIndex)

				logs = append(logs, log)

				txLogIndex++
				logIndex++
			}
			receipt.Logs = logs // replace the origin logs

			receipts[tx.Hash] = receipt
		}
	}

	return EpochData{epochNumber, blocks, receipts}, nil
}
