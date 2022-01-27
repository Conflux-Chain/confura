package store

import (
	"fmt"

	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// EthData wraps the blockchain data of an ETH block.
type EthData struct {
	Number   uint64                             // block number
	Block    *web3Types.Block                   // block body
	Receipts map[common.Hash]*web3Types.Receipt // receipts
}

// IsContinuousTo checks if this block is continuous to the previous block.
func (current *EthData) IsContinuousTo(prev *EthData) (continuous bool, desc string) {
	if current.Block.ParentHash != prev.Block.Hash {
		desc = fmt.Sprintf(
			"parent hash not matched, expect %v got %v", prev.Block.Hash, current.Block.ParentHash,
		)
		return
	}

	if prev.Number+1 != current.Number {
		desc = fmt.Sprintf(
			"block number not continuous, expect %v got %v", prev.Number+1, current.Number,
		)
		return
	}

	return true, ""
}

// ToEpochData converts ETH block data to Conflux epoch data. This is used for bridge eth
// block data with epoch data to reduce redundant codes eg., store logic.
func (current *EthData) ToEpochData() (*EpochData, error) {
	// TODO: convert ETH block data to epoch data
	return nil, errors.New("not implemented yet")
}

// QueryEthData queries blockchain data for the specified block number.
func QueryEthData(w3c *web3go.Client, blockNumber uint64) (*EthData, error) {
	updater := metrics.NewTimerUpdaterByName("infura/duration/store/eth/query")
	defer updater.Update()

	// Get block by number
	block, err := w3c.Eth.BlockByNumber(web3Types.BlockNumber(blockNumber), true)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to get block by number %v", blockNumber)
	}

	logger := logrus.WithFields(logrus.Fields{
		"blockHash": block.Hash, "blockNumber": blockNumber,
	})

	txReceipts := map[common.Hash]*web3Types.Receipt{}
	blockTxs := block.Transactions.Transactions()

	// TODO: improve performance with batch block receipts
	for i := 0; i <= len(blockTxs); i++ {
		txHash := blockTxs[i].Hash
		blogger := logger.WithFields(logrus.Fields{"txHash": txHash, "i": i})

		receipt, err := w3c.Eth.TransactionReceipt(txHash)
		if err != nil {
			blogger.WithError(err).Info("Failed to query ETH transaction receipt")
			return nil, errors.WithMessage(err, "failed to get receipt for tx")
		}

		// sanity check in case of chain re-org
		switch {
		case receipt == nil: // receipt shouldn't be nil unless chain re-org
			err = errors.WithMessage(ErrEpochPivotSwitched, "receipt nil")
		case receipt.BlockHash != block.Hash:
			blogger = blogger.WithFields(logrus.Fields{
				"rcptBlockHash": receipt.BlockHash, "expectedBlockHash": block.Hash,
			})
			err = errors.WithMessage(ErrEpochPivotSwitched, "receipt block hash mismatch")
		case receipt.BlockNumber != blockNumber:
			blogger = blogger.WithFields(logrus.Fields{
				"rcptBlockNumber": receipt.BlockNumber, "expectedBlockNumber": blockNumber,
			})
			err = errors.WithMessage(ErrEpochPivotSwitched, "receipt block number mismatch")
		case receipt.TransactionHash != txHash:
			blogger = blogger.WithField("rcptTxHash", receipt.TransactionHash)
			err = errors.WithMessage(ErrEpochPivotSwitched, "receipt tx hash mismatch")
		}

		if err != nil {
			blogger.WithError(err).Info("Failed to query ETH transaction receipt due to chain re-org")
			return nil, err
		}

		txReceipts[txHash] = receipt
	}

	return &EthData{blockNumber, block, txReceipts}, nil
}
