package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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
	// Initialize `DefaultReceiptOption` with config values from viper
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

	if bh, ok := bnh.Hash(); ok {
		return w3c.WithContext(ctx).Eth.BlockByHash(bh, isFull)
	}

	return nil, errors.New("invalid block number or hash")
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

	// If the method is not a concrete one, perform method detection
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
		return nil, errors.Errorf("unsupported receipt method: %v", opt.Method)
	}
}

type QueryOption struct {
	ReceiptConfig EthReceiptOption
	Disabler      ChainDataDisabler
}

// QueryEthData queries blockchain data for the specified block number.
func QueryEthData(
	ctx context.Context,
	w3c *web3go.Client,
	blockNumber uint64,
	opts ...QueryOption,
) (*EthData, error) {
	startTime := time.Now()
	defer metrics.Registry.Sync.QueryEpochData("eth").UpdateSince(startTime)

	var opt QueryOption
	if len(opts) > 0 {
		opt = opts[0]
	} else {
		opt = QueryOption{
			ReceiptConfig: DefaultReceiptOption,
			Disabler:      &ethStoreConfig,
		}
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
	opt QueryOption,
) (*EthData, error) {
	// Retrieve the block by number
	block, err := w3c.Eth.BlockByNumber(types.BlockNumber(blockNumber), true)

	if err == nil && block == nil {
		err = errors.New("invalid block data (must not be nil)")
	}

	if err != nil {
		return nil, errors.WithMessagef(err, "failed to get block by number %v", blockNumber)
	}

	txnReceipts := map[common.Hash]*types.Receipt{}
	blockTxs := block.Transactions.Transactions()

	// Check if ChainReceipts are disabled
	if opt.Disabler != nil && opt.Disabler.IsChainReceiptDisabled() {
		// If both ChainReceipt and ChainLog are disabled, return only the block data
		if opt.Disabler.IsChainLogDisabled() {
			return &EthData{
				Number: blockNumber,
				Block:  block,
			}, nil
		}

		// Retrieve logs for the block
		logs, err := w3c.Eth.Logs(types.FilterQuery{BlockHash: &block.Hash})
		if err != nil {
			return nil, errors.WithMessage(err, "failed to get block event logs")
		}

		// Map logs to their respective transaction hashes
		txnLogs := map[common.Hash][]*types.Log{}
		for i := range logs {
			txnHash := logs[i].TxHash
			txnLogs[txnHash] = append(txnLogs[txnHash], &logs[i])
		}

		// Construct minimal receipts with logs for each transaction
		for i, tx := range blockTxs {
			txnReceipts[tx.Hash] = &types.Receipt{
				BlockHash:        block.Hash,
				BlockNumber:      blockNumber,
				TransactionHash:  tx.Hash,
				TransactionIndex: uint64(i),
				Logs:             txnLogs[tx.Hash],
			}
		}

		return &EthData{
			Number:   blockNumber,
			Block:    block,
			Receipts: txnReceipts,
		}, nil
	}

	// Set the block as prefetched data to avoid redundant queries
	opt.ReceiptConfig.Prefetched = block

	// Retrieve full receipts for the block
	blockNumOrHash := types.BlockNumberOrHashWithNumber(types.BlockNumber(blockNumber))
	blockReceipts, err := QueryEthReceipt(ctx, w3c, blockNumOrHash, opt.ReceiptConfig)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get block receipts")
	}

	// Ensure the number of receipts matches the number of transactions
	if len(blockReceipts) != len(blockTxs) {
		return nil, errors.Errorf(
			"mismatch in number of transactions and receipts: %d transactions, %d receipts for block %v",
			len(blockTxs), len(blockReceipts), block.Hash,
		)
	}

	// Verify each receipt and map it to its transaction hash
	for i, tx := range blockTxs {
		txnHash := tx.Hash
		receipt := blockReceipts[i]

		if err := verifyEthTxnReceipt(block, txnHash, receipt); err != nil {
			return nil, errors.WithMessagef(err, "failed to verify txn receipt for txn %v", txnHash)
		}

		txnReceipts[txnHash] = receipt
	}

	return &EthData{
		Number:   blockNumber,
		Block:    block,
		Receipts: txnReceipts,
	}, nil
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

func verifyEthTxnReceipt(block *types.Block, txnHash common.Hash, receipt *types.Receipt) error {
	// Check for potential chain reorg or invalid data
	switch {
	case receipt == nil:
		return errors.WithMessagef(
			ErrChainReorged,
			"nil receipt for txn (%v) in block (%v)",
			txnHash, block.Hash,
		)
	case receipt.BlockHash != block.Hash:
		return errors.WithMessagef(
			ErrChainReorged,
			"block hash mismatch: receipt has %v, expected %v",
			receipt.BlockHash, block.Hash,
		)
	case receipt.BlockNumber != block.Number.Uint64():
		return errors.WithMessagef(
			ErrChainReorged,
			"block number mismatch: receipt has #%v, expected #%v",
			receipt.BlockNumber, block.Number.Uint64(),
		)
	case txnHash != receipt.TransactionHash:
		return errors.WithMessagef(
			ErrChainReorged,
			"txn hash mismatch: receipt has %v, expected %v in block %v",
			receipt.TransactionHash, txnHash, block.Hash,
		)
	default:
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
			return nil, errors.WithMessagef(err, "failed to get block (%v)", bnh)
		}
	}
	txnHashes := getEthBlockTxnHashes(block)

	if rcptOpt.Concurrency <= 1 { // No parallelism
		for i := 0; i < len(txnHashes); i++ {
			receipt, err := w3c.Eth.TransactionReceipt(txnHashes[i])
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"blockHash":   block.Hash,
					"blockNumber": block.Number.Int64(),
					"txnHash":     txnHashes[i],
					"txnIndex":    i,
				}).WithError(err).Debug("Failed to query eth transaction receipt")
				return nil, wrapReceiptRetrievalError(err, txnHashes[i])
			}

			if err := verifyEthTxnReceipt(block, txnHashes[i], receipt); err != nil {
				return nil, errors.WithMessage(err, "failed to verify transaction receipt")
			}
			receipts = append(receipts, receipt)
		}

		return receipts, nil
	}

	errGrp, ctx := errgroup.WithContext(ctx)
	errGrp.SetLimit(rcptOpt.Concurrency)

	var breakLoop bool
	receipts = make([]*types.Receipt, len(txnHashes))
	for idx := 0; !breakLoop && idx < len(txnHashes); idx++ {
		// Capture loop variables
		txnHash, rcptIdx := txnHashes[idx], idx

		// Start goroutine for each transaction
		errGrp.Go(func() error {
			receipt, err := w3c.WithContext(ctx).Eth.TransactionReceipt(txnHash)
			if err != nil {
				return wrapReceiptRetrievalError(err, txnHash)
			}

			if err := verifyEthTxnReceipt(block, txnHash, receipt); err != nil {
				return errors.WithMessage(err, "failed to verify transaction receipt")
			}

			// Thread safe to write here since slice index is unique for each goroutine within the group.
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
