package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type EthReceiptMethod uint32

const (
	EthReceiptMethodAutoDetect = iota
	EthReceiptMethodEthBlockReceipts
	EthReceiptMethodParityBlockReceipts
	EthReceiptMethodEthTxnReceipt
	EthReceiptMethodEnd
)

// String returns the string representation of the EthReceiptMethod
func (m EthReceiptMethod) String() string {
	switch m {
	case EthReceiptMethodAutoDetect:
		return "auto detect"
	case EthReceiptMethodEthBlockReceipts:
		return "eth_blockReceipts"
	case EthReceiptMethodParityBlockReceipts:
		return "parity_blockReceipts"
	case EthReceiptMethodEthTxnReceipt:
		return "eth_getTransactionReceipt"
	default:
		return fmt.Sprintf("unknown receipt method (%d)", m)
	}
}

// IsConcrete returns true if the method is a concrete receipt retrieval method (excluding auto-detect).
func (m EthReceiptMethod) IsConcrete() bool {
	return m > EthReceiptMethodAutoDetect && m < EthReceiptMethodEnd
}

type receiptRetriever func(context.Context, *web3go.Client, types.BlockNumberOrHash, EthReceiptOption) ([]*types.Receipt, error)

var (
	DefaultReceiptOption      EthReceiptOption
	defaultRcptMethodDetector EthReceiptMethodDetector

	// Prioritized methods in order to detect the best receipt retrieval method
	receiptRetrievalMethods = []struct {
		method EthReceiptMethod
		query  receiptRetriever
	}{
		{EthReceiptMethodEthBlockReceipts, queryEthReceiptByEthBlockReceipts},
		{EthReceiptMethodParityBlockReceipts, queryEthReceiptByParityBlockReceipts},
		{EthReceiptMethodEthTxnReceipt, queryEthReceiptByEthTxnReceipt},
	}
)

func initEth() {
	// Initialize DefaultEthOption with config values from viper
	viper.MustUnmarshalKey(
		"requestControl.ethReceiptRetrieval", &DefaultReceiptOption,
	)
}

type EthReceiptOption struct {
	Method      EthReceiptMethod
	Concurrency int

	// Pre-fetched data can be used to accelerate the query eg., block header etc.
	Prefetched interface{}
}

// EthReceiptMethodDetector detects the best method for retrieving Ethereum receipts.
// The detection occurs only once to optimize performance by caching the result.
type EthReceiptMethodDetector struct {
	mu     sync.Mutex
	method EthReceiptMethod // Cached method
}

// Detect determines the best receipt retrieval method.
// If a method has already been detected, it returns the cached method.
// Otherwise, it attempts to detect the best method.
func (d *EthReceiptMethodDetector) Detect(
	ctx context.Context,
	client *web3go.Client,
	bnh types.BlockNumberOrHash,
) (EthReceiptMethod, error) {
	// Check if method was already detected
	if method := d.load(); method.IsConcrete() {
		return method, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Double-check
	if method := d.load(); method.IsConcrete() {
		return method, nil
	}

	// Perform best method detection
	method, err := d.detect(ctx, client, bnh)
	if err != nil {
		return 0, wrapDetectionError(err)
	}

	// Cache the detected method for future calls
	d.store(method)

	logrus.WithField("method", method).Debug("Best receipt retrieval method detected")
	return method, nil
}

// load returns the cached method.
func (d *EthReceiptMethodDetector) load() EthReceiptMethod {
	method := atomic.LoadUint32((*uint32)(&d.method))
	return EthReceiptMethod(method)
}

// store caches the detected method.
func (d *EthReceiptMethodDetector) store(method EthReceiptMethod) {
	atomic.StoreUint32((*uint32)(&d.method), uint32(method))
}

// detect attempts to detect the best receipt retrieval method by trying several options.
func (d *EthReceiptMethodDetector) detect(
	ctx context.Context, client *web3go.Client, blockNunOrHash types.BlockNumberOrHash,
) (EthReceiptMethod, error) {
	for _, m := range receiptRetrievalMethods {
		_, err := m.query(ctx, client, blockNunOrHash, DefaultReceiptOption)
		if err == nil || errors.Is(err, ErrChainReorged) {
			return m.method, nil
		}

		if !utils.IsRPCJSONError(err) {
			// Potential I/O error
			return 0, wrapDetectionError(err)
		}
	}

	return 0, wrapDetectionError(errors.New("no available method succeeded"))
}

// wrapDetectionError adds context to receipt method detection errors
func wrapDetectionError(err error) error {
	return errors.WithMessage(err, "failed to detect receipt retrieval method")
}

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

func wrapReceiptMethodNotSupportedError(method EthReceiptMethod) error {
	return errors.Errorf("receipt retrieval method %v not supported", method)
}

func QueryEthReceipt(
	ctx context.Context,
	w3c *web3go.Client,
	bnh types.BlockNumberOrHash,
	opts ...EthReceiptOption,
) ([]*types.Receipt, error) {
	// Use the default option unless an override is provided
	opt := DefaultReceiptOption
	if len(opts) > 0 {
		opt = opts[0]
	}

	// If the method is set to auto-detect or invalid, perform detection
	if !opt.Method.IsConcrete() {
		method, err := defaultRcptMethodDetector.Detect(ctx, w3c, bnh)
		if err != nil {
			return nil, err
		}
		opt.Method = method
	}

	// Retrieve receipts based on the specified method
	switch opt.Method {
	case EthReceiptMethodParityBlockReceipts:
		return queryEthReceiptByParityBlockReceipts(ctx, w3c, bnh, opt)
	case EthReceiptMethodEthTxnReceipt:
		return queryEthReceiptByEthTxnReceipt(ctx, w3c, bnh, opt)
	case EthReceiptMethodEthBlockReceipts:
		return queryEthReceiptByEthBlockReceipts(ctx, w3c, bnh, opt)
	default:
		return nil, wrapReceiptMethodNotSupportedError(opt.Method)
	}
}

// QueryEthData queries blockchain data for the specified block number.
func QueryEthData(
	ctx context.Context,
	w3c *web3go.Client,
	blockNumber uint64,
	opts ...EthReceiptOption,
) (*EthData, error) {
	updater := metrics.Registry.Sync.QueryEpochData("eth")
	defer updater.Update()

	opt := DefaultReceiptOption
	if len(opts) > 0 {
		opt = opts[0]
	}

	data, err := queryEthData(ctx, w3c, blockNumber, opt)
	metrics.Registry.Sync.QueryEpochDataAvailability("eth").
		Mark(err == nil || errors.Is(err, ErrChainReorged))

	return data, err
}

func queryEthData(
	ctx context.Context,
	w3c *web3go.Client,
	blockNumber uint64,
	rcptOpt EthReceiptOption,
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
		if err := verifyEthTxnReceipt(block, i, receipt); err != nil {
			return nil, errors.WithMessage(err, "failed to verify txn receipt")
		}

		txReceipts[blockTxs[i].Hash] = receipt
	}

	return &EthData{blockNumber, block, txReceipts}, nil
}

func queryEthReceiptByParityBlockReceipts(
	ctx context.Context,
	w3c *web3go.Client,
	bnh types.BlockNumberOrHash,
	rcptOpt EthReceiptOption,
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

func wrapReceiptRetrievalError(cause error, txnHash common.Hash) error {
	return errors.WithMessagef(cause, "failed to query receipt for txn %v", txnHash)
}

func verifyEthTxnReceipt(block *types.Block, txnIndex int, receipt *types.Receipt) error {
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
		txnHashes := getEthBlockTxnHashes(block)
		if len(txnHashes) <= txnIndex {
			return errors.WithMessagef(
				ErrChainReorged, "failed to match txn %v within block %v due to index %v out of bound",
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

func getEthBlockTxnHashes(block *types.Block) (txnHashes []common.Hash) {
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
	rcptOpt EthReceiptOption,
) (receipts []*types.Receipt, err error) {
	block, ok := rcptOpt.Prefetched.(*types.Block)
	if !ok || block == nil {
		block, err = GetBlockByBlockNumberOrHash(ctx, w3c, bnh, false)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to get block by block number or hash")
		}
	}
	txnHashes := getEthBlockTxnHashes(block)

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
				return nil, wrapReceiptRetrievalError(err, txnHashes[i])
			}

			if err := verifyEthTxnReceipt(block, i, receipt); err != nil {
				return nil, errors.WithMessage(err, "failed to verify transaction receipt")
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
				return wrapReceiptRetrievalError(err, txnHash)
			}

			if err := verifyEthTxnReceipt(block, rcptIdx, receipt); err != nil {
				return errors.WithMessage(err, "failed to verify transaction receipt")
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
	rcptOpt EthReceiptOption,
) ([]*types.Receipt, error) {
	blockReceipts, err := w3c.WithContext(ctx).Eth.BlockReceipts(&bnh)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get eth block receipts")
	}

	return blockReceipts, nil
}
