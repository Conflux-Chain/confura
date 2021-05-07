package store

import (
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/metrics"
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
	updater := metrics.NewTimerUpdaterByName("infura/store/epoch/query")
	defer updater.Update()

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

	// Batch get epoch receipts
	epochReceipts, err := cfx.GetEpochReceipts(*epoch)
	if err != nil {
		return EpochData{}, errors.WithMessagef(err, "Failed to get epoch receipts for epoch %v", epoch)
	}

	receipts := make(map[types.Hash]*types.TransactionReceipt)
	var logIndex uint64

	for i, block := range blocks {
		for j, tx := range block.Transactions {
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

			// Find transaction receipts from above batch retrieved receipts
			// We assume the order of batched retrieved receipts is the same with
			// the fetched blocks & transactions. Even so, we still need to do some
			// bound checking and fault tolerance to be robust.
			if i >= len(epochReceipts) || j >= len(epochReceipts[i]) {
				logrus.WithFields(logrus.Fields{
					"epoch": epochNumber,
					"block": block.Hash.String(),
					"tx":    tx.Hash.String(),
				}).Error("Batch retrieved receipts out of bound")

				continue
			}

			receipt := &epochReceipts[i][j]

			if receipt == nil {
				logrus.WithFields(logrus.Fields{
					"epoch": epochNumber,
					"block": block.Hash.String(),
					"tx":    tx.Hash.String(),
				}).Error("Transaction receipt not found")

				continue
			}

			// Check receipt epoch number
			if uint64(*receipt.EpochNumber) != epochNumber {
				logrus.WithFields(logrus.Fields{
					"epoch": epochNumber,
					"block": block.Hash.String(),
					"tx":    tx.Hash.String(),
				}).Errorf("Receipt epoch number mismatch, value = %v", uint64(*receipt.EpochNumber))

				continue
			}

			// Check receipt block hash
			if receipt.BlockHash.String() != block.Hash.String() {
				logrus.WithFields(logrus.Fields{
					"epoch": epochNumber,
					"block": block.Hash.String(),
					"tx":    tx.Hash.String(),
				}).Errorf("Receipt block hash mismatch, value = %v", receipt.BlockHash.String())

				continue
			}

			// Check receipt transaction hash
			if receipt.TransactionHash.String() != tx.Hash.String() {
				logrus.WithFields(logrus.Fields{
					"epoch": epochNumber,
					"block": block.Hash.String(),
					"tx":    tx.Hash.String(),
				}).Errorf("Receipt transaction hash mismatch, value = %v", receipt.TransactionHash.String())

				continue
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
