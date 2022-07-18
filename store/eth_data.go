package store

import (
	"fmt"

	"github.com/ethereum/go-ethereum/rpc"

	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// EthData wraps the evm space blockchain data.
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

// QueryEthData queries blockchain data for the specified block number.
func QueryEthData(w3c *web3go.Client, blockNumber uint64, useBatch bool) (*EthData, error) {
	updater := metrics.Registry.Sync.QueryEpochData("eth")
	defer updater.Update()

	data, err := queryEthData(w3c, blockNumber, useBatch)
	metrics.Registry.Sync.QueryEpochDataAvailability("eth").
		Mark(err == nil || errors.Is(err, ErrChainReorged))

	return data, err
}

func queryEthData(w3c *web3go.Client, blockNumber uint64, useBatch bool) (*EthData, error) {
	// Get block by number
	block, err := w3c.Eth.BlockByNumber(web3Types.BlockNumber(blockNumber), true)

	if err == nil && block == nil {
		err = errors.New("invalid block data (must not be nil)")
	}

	if err != nil {
		return nil, errors.WithMessagef(err, "failed to get block by number %v", blockNumber)
	}

	logger := logrus.WithFields(logrus.Fields{
		"blockHash": block.Hash, "blockNumber": blockNumber,
	})

	var blockReceipts []web3Types.Receipt
	if useBatch {
		// Batch get block receipts.
		blockNumOrHash := web3Types.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumber))
		blockReceipts, err = w3c.Parity.BlockReceipts(&blockNumOrHash)
		if err != nil {
			logger.WithError(err).Info("Failed to batch query ETH block receipts")
			return nil, errors.WithMessage(err, "failed to get block receipts")
		}
	}

	txReceipts := map[common.Hash]*web3Types.Receipt{}
	blockTxs := block.Transactions.Transactions()

	for i := 0; i < len(blockTxs); i++ {
		txHash := blockTxs[i].Hash
		blogger := logger.WithFields(logrus.Fields{"txHash": txHash, "i": i})

		var receipt *web3Types.Receipt
		if useBatch {
			if blockReceipts == nil {
				blogger.Info("Failed to match tx receipts due to block receipts nil (regarded as chain reorg)")
				return nil, errors.WithMessage(ErrChainReorged, "batch retrieved block receipts nil")
			}

			if i >= len(blockReceipts) {
				blogger.WithField("blockReceipts", blockReceipts).Info(
					"Failed to match tx receipts due to block receipts out of bound",
				)
				return nil, errors.New("batch retrieved block receipts out of bound")
			}

			receipt = &blockReceipts[i]
		} else {
			receipt, err = w3c.Eth.TransactionReceipt(txHash)
			if err != nil {
				blogger.WithError(err).Info("Failed to query ETH transaction receipt")
				return nil, errors.WithMessagef(err, "failed to get receipt for tx %v", txHash)
			}
		}

		// sanity check in case of chain re-org
		switch {
		case receipt == nil: // receipt shouldn't be nil unless chain re-org
			err = errors.WithMessage(ErrChainReorged, "tx receipt nil")
		case receipt.BlockHash != block.Hash:
			blogger = blogger.WithFields(logrus.Fields{
				"rcptBlockHash": receipt.BlockHash, "expectedBlockHash": block.Hash,
			})
			err = errors.WithMessage(ErrChainReorged, "receipt block hash mismatch")
		case receipt.BlockNumber != blockNumber:
			blogger = blogger.WithFields(logrus.Fields{
				"rcptBlockNumber": receipt.BlockNumber, "expectedBlockNumber": blockNumber,
			})
			err = errors.WithMessage(ErrChainReorged, "receipt block number mismatch")
		case receipt.TransactionHash != txHash:
			blogger = blogger.WithField("rcptTxHash", receipt.TransactionHash)
			err = errors.WithMessage(ErrChainReorged, "receipt tx hash mismatch")
		}

		if err != nil {
			blogger.WithError(err).Info("Failed to query ETH transaction receipt (regarded as chain re-org)")
			return nil, err
		}

		txReceipts[txHash] = receipt
	}

	return &EthData{blockNumber, block, txReceipts}, nil
}
