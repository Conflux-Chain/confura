package store

import (
	"context"
	"fmt"

	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var (
	ErrReceiptRetrievalMethodNotSupported = errors.New("receipt retrieval method not supported")
)

type EthReceiptRetrievalMethod int

const (
	EthReceiptRetrievalMethodParityBlockReceipts = iota
	EthReceiptRetrievalMethodEthTxnReceipt
	EthReceiptRetrievalMethodEthBlockReceipts
)

// EthData wraps the evm space blockchain data.
type EthData struct {
	Number   uint64                         // block number
	Block    *types.Block                   // block body
	Receipts map[common.Hash]*types.Receipt // receipts
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

func GetBlockByBlockNumberOrHash(
	ctx context.Context,
	w3c *web3go.Client,
	bnh types.BlockNumberOrHash,
	isFull bool,
) (*types.Block, error) {
	if bn, ok := bnh.Number(); ok {
		return w3c.WithContext(ctx).Eth.BlockByNumber(bn, isFull)
	}

	return w3c.WithContext(ctx).Eth.BlockByHash(*bnh.BlockHash, isFull)
}

type EthReceiptRetrievalOption struct {
	Method      EthReceiptRetrievalMethod
	Concurrency int

	// pre-fetched data can be used to accelerate the query eg., block header etc.
	Prefetched interface{}
}

func QueryEthReceipt(
	ctx context.Context,
	w3c *web3go.Client,
	bnh types.BlockNumberOrHash,
	opt EthReceiptRetrievalOption,
) ([]*types.Receipt, error) {
	switch opt.Method {
	case EthReceiptRetrievalMethodParityBlockReceipts:
		return queryEthReceiptByParityBlockReceipts(ctx, w3c, bnh)
	case EthReceiptRetrievalMethodEthTxnReceipt:
		return queryEthReceiptByEthTxnReceipt(ctx, w3c, bnh, opt)
	case EthReceiptRetrievalMethodEthBlockReceipts:
		return queryEthReceiptByEthBlockReceipts(ctx, w3c, bnh)
	default:
		return nil, ErrReceiptRetrievalMethodNotSupported
	}
}

// QueryEthData queries blockchain data for the specified block number.
func QueryEthData(
	ctx context.Context,
	w3c *web3go.Client,
	blockNumber uint64,
	rcptOpt EthReceiptRetrievalOption,
) (*EthData, error) {
	updater := metrics.Registry.Sync.QueryEpochData("eth")
	defer updater.Update()

	data, err := queryEthData(ctx, w3c, blockNumber, rcptOpt)
	metrics.Registry.Sync.QueryEpochDataAvailability("eth").
		Mark(err == nil || errors.Is(err, ErrChainReorged))

	return data, err
}

func queryEthData(
	ctx context.Context,
	w3c *web3go.Client,
	blockNumber uint64,
	rcptOpt EthReceiptRetrievalOption,
) (*EthData, error) {
	// Get block by number
	block, err := w3c.Eth.BlockByNumber(types.BlockNumber(blockNumber), true)

	if err == nil && block == nil {
		err = errors.New("invalid block data (must not be nil)")
	}

	if err != nil {
		return nil, errors.WithMessagef(err, "failed to get block by number %v", blockNumber)
	}

	// Set the block as prefetched data so that no need to query it again.
	rcptOpt.Prefetched = block

	blockNumOrHash := types.BlockNumberOrHashWithNumber(types.BlockNumber(blockNumber))
	blockReceipts, err := QueryEthReceipt(ctx, w3c, blockNumOrHash, rcptOpt)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get block receipts")
	}

	if blockReceipts == nil {
		return nil, errors.WithMessage(ErrChainReorged, "block receipts nil")
	}

	txReceipts := map[common.Hash]*types.Receipt{}
	blockTxs := block.Transactions.Transactions()

	for i := 0; i < len(blockTxs); i++ {
		receipt := blockReceipts[i]
		if err := validateEthTxnReceipt(block, i, receipt); err != nil {
			return nil, errors.WithMessage(err, "failed to validate txn receipt")
		}

		txReceipts[blockTxs[i].Hash] = receipt
	}

	return &EthData{blockNumber, block, txReceipts}, nil
}

func queryEthReceiptByParityBlockReceipts(
	ctx context.Context,
	w3c *web3go.Client,
	bnh types.BlockNumberOrHash,
) (receipts []*types.Receipt, err error) {
	blockReceipts, err := w3c.WithContext(ctx).Parity.BlockReceipts(&bnh)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get parity block receipts")
	}

	for i := range blockReceipts {
		receipts = append(receipts, &blockReceipts[i])
	}

	return receipts, nil
}

func newEthTxnRetrievalError(cause error, txnHash common.Hash) error {
	return errors.WithMessagef(cause, "failed to query receipt for txn %v", txnHash)
}

func validateEthTxnReceipt(block *types.Block, txnIndex int, receipt *types.Receipt) error {
	// sanity check in case of chain re-org
	switch {
	case receipt == nil: // receipt shouldn't be nil unless chain re-org
		return errors.WithMessagef(
			ErrChainReorged, "nil txn receipt for block %v at index %v", block.Hash, txnIndex,
		)
	case receipt.BlockHash != block.Hash:
		return errors.WithMessagef(
			ErrChainReorged, "receipt block hash %v mismatch for block %v at index %v",
			receipt.BlockHash, block.Hash, txnIndex,
		)
	case receipt.BlockNumber != block.Number.Uint64():
		return errors.WithMessagef(
			ErrChainReorged, "receipt block number #%v mismatch for block #%v at index %v",
			receipt.BlockNumber, block.Nonce.Uint64(), txnIndex,
		)
	default:
		txnHashes := getBlockTxnHashes(block)
		if len(txnHashes) <= txnIndex {
			return errors.Errorf(
				"failed to match txn %v within block %v due to index %v out of bound",
				receipt.TransactionHash, block.Hash, txnIndex,
			)
		}
		if th := txnHashes[txnIndex]; th != receipt.TransactionHash {
			return errors.WithMessagef(
				ErrChainReorged, "receipt tx hash %v mismatch for txn %v within block %v at index %v",
				receipt.TransactionHash, th, block.Hash, txnIndex,
			)
		}
		return nil
	}
}

func getBlockTxnHashes(block *types.Block) (txnHashes []common.Hash) {
	if block.Transactions.Type() == types.TXLIST_HASH {
		return block.Transactions.Hashes()
	}

	txns := block.Transactions.Transactions()
	for i := 0; i < len(txns); i++ {
		txnHashes = append(txnHashes, txns[i].Hash)
	}
	return txnHashes
}

func queryEthReceiptByEthTxnReceipt(
	ctx context.Context,
	w3c *web3go.Client,
	bnh types.BlockNumberOrHash,
	rcptOpt EthReceiptRetrievalOption,
) (receipts []*types.Receipt, err error) {
	block, ok := rcptOpt.Prefetched.(*types.Block)
	if !ok || block == nil {
		block, err = GetBlockByBlockNumberOrHash(ctx, w3c, bnh, false)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to get block by block number or hash")
		}
	}
	txnHashes := getBlockTxnHashes(block)

	if rcptOpt.Concurrency == 0 {
		for i := 0; i < len(txnHashes); i++ {
			receipt, err := w3c.Eth.TransactionReceipt(txnHashes[i])
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"blockHash":   block.Hash,
					"blockNumber": block.Number.Int64(),
					"txnHash":     txnHashes[i],
					"txnIndex":    i,
				}).WithError(err).Info("Failed to query eth transaction receipt")
				return nil, newEthTxnRetrievalError(err, txnHashes[i])
			}

			if err := validateEthTxnReceipt(block, i, receipt); err != nil {
				return nil, errors.WithMessage(err, "failed to validate transaction receipt")
			}
			receipts = append(receipts, receipt)
		}

		return receipts, nil
	}

	errGrp, ctx := errgroup.WithContext(ctx)
	errGrp.SetLimit(rcptOpt.Concurrency)

	breakLoop, receipts := false, make([]*types.Receipt, len(txnHashes))
	for idx := 0; !breakLoop && idx < len(txnHashes); idx++ {
		// Capture loop variables
		txnHash, rcptIdx := txnHashes[idx], idx

		// Start goroutine for each transaction
		errGrp.Go(func() error {
			receipt, err := w3c.WithContext(ctx).Eth.TransactionReceipt(txnHash)
			if err != nil {
				return newEthTxnRetrievalError(err, txnHash)
			}

			if err := validateEthTxnReceipt(block, rcptIdx, receipt); err != nil {
				return errors.WithMessage(err, "failed to validate transaction receipt")
			}

			// Thread safe to write here since receipt index is unique for each goroutine within the group.
			receipts[rcptIdx] = receipt
			return nil
		})

		select {
		case <-ctx.Done():
			// Break the loop if any error happened
			breakLoop = true
		default:
		}
	}

	if err := errGrp.Wait(); err != nil {
		return nil, err
	}

	return receipts, nil
}

func queryEthReceiptByEthBlockReceipts(
	ctx context.Context,
	w3c *web3go.Client,
	bnh types.BlockNumberOrHash,
) ([]*types.Receipt, error) {
	blockReceipts, err := w3c.WithContext(ctx).Eth.BlockReceipts(&bnh)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get eth block receipts")
	}

	return blockReceipts, nil
}
