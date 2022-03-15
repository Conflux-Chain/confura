package store

import (
	"fmt"
	"strings"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	sdkerr "github.com/Conflux-Chain/go-conflux-sdk/types/errors"
	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	// flyweight instance for empty EpochData
	emptyEpochData = EpochData{}

	// blacklisted contract address set
	blacklistedAddressSet = map[string]struct{}{}

	errBlockValidationFailed      = errors.New("epoch block validation failed")
	errTxsReceiptValidationFailed = errors.New("transaction receipt validation failed")
)

func init() {
	// Load blacklisted contract address.
	blAddrs := viper.GetStringSlice("sync.addrBlacklist")
	if len(blAddrs) > 0 { // setup blacklisted address set
		for _, addr := range blAddrs {
			blacklistedAddressSet[strings.ToLower(addr)] = struct{}{}
		}
	}
}

// EpochData wraps the blockchain data of an epoch.
type EpochData struct {
	Number   uint64         // epoch number
	Blocks   []*types.Block // blocks in order and the last one is pivot block
	Receipts map[types.Hash]*types.TransactionReceipt

	// custom extra extentions
	BlockExts   []*BlockExtra
	ReceiptExts map[types.Hash]*ReceiptExtra
}

// CalculateDbRows calculates to be inserted db rows
func (epoch *EpochData) CalculateDbRows() int {
	dbRows := 0

	storeDisabler := StoreConfig()
	if !storeDisabler.IsChainBlockDisabled() {
		dbRows += len(epoch.Blocks)
	}

	if !storeDisabler.IsChainReceiptDisabled() || !storeDisabler.IsChainTxnDisabled() {
		dbRows += len(epoch.Receipts)
	}

	if !storeDisabler.IsChainLogDisabled() {
		for _, rcpt := range epoch.Receipts {
			dbRows += len(rcpt.Logs)
		}
	}

	return dbRows
}

func (epoch *EpochData) GetPivotBlock() *types.Block {
	return epoch.Blocks[len(epoch.Blocks)-1]
}

// IsContinuousTo checks if this epoch is continuous to the previous epoch.
func (epoch *EpochData) IsContinuousTo(prev *EpochData) (continuous bool, desc string) {
	lastPivot := prev.GetPivotBlock()
	nextPivot := epoch.GetPivotBlock()

	if lastPivot.EpochNumber == nil || nextPivot.EpochNumber == nil {
		desc = "epoch number nil"
		return
	}

	if lastPivot.Hash != nextPivot.ParentHash {
		desc = fmt.Sprintf(
			"parent hash not matched, expect %v got %v", lastPivot.Hash, nextPivot.ParentHash,
		)
		return
	}

	lastEpochNo := lastPivot.EpochNumber.ToInt().Uint64()
	nextEpochNo := nextPivot.EpochNumber.ToInt().Uint64()
	if lastEpochNo+1 != nextEpochNo {
		desc = fmt.Sprintf(
			"epoch number not continuous, expect %v got %v", lastEpochNo+1, nextEpochNo,
		)
		return
	}

	continuous = true
	return
}

// QueryEpochData queries blockchain data for the specified epoch number.
// TODO better to use batch API to return all if performance is low in case of high TPS.
func QueryEpochData(cfx sdk.ClientOperator, epochNumber uint64, useBatch bool) (EpochData, error) {
	updater := metrics.NewTimerUpdaterByName("infura/duration/store/epoch/query")
	defer updater.Update()

	// Get epoch block hashes.
	epoch := types.NewEpochNumberUint64(epochNumber)
	blockHashes, err := cfx.GetBlocksByEpoch(epoch)

	if err == nil && len(blockHashes) == 0 { // invalid epoch data, must have at lease one block
		err = errors.New("invalid epoch data (must have at least one block)")
	}

	if err != nil {
		return emptyEpochData, errors.WithMessagef(err, "failed to get blocks by epoch %v", epochNumber)
	}

	pivotHash := blockHashes[len(blockHashes)-1]
	blocks := make([]*types.Block, 0, len(blockHashes))

	logger := logrus.WithFields(logrus.Fields{
		"epochNo": epochNumber, "pivotHash": pivotHash,
	})

	// Fetch epoch block summary one by one.
	anyBlockExecuted := false
	for _, hash := range blockHashes {
		block, err := cfx.GetBlockByHashWithPivotAssumption(hash, pivotHash, hexutil.Uint64(epochNumber))
		if err == nil { // validate block first if no error
			err = validateBlock(&block, epochNumber, hash)
		}

		if checkPivotSwitchWithError(err) { // check pivot switch
			l := logger.WithFields(
				logrus.Fields{"blockHash": hash, "blockHeader": &(block.BlockHeader)},
			)
			l.WithError(err).Info(
				"Failed to get block by hash with pivot assumption (regarded as pivot switch)",
			)

			err = ErrEpochPivotSwitched
		}

		if err != nil {
			return emptyEpochData, errors.WithMessagef(err, "failed to get block by hash %v", hash)
		}

		if !anyBlockExecuted && !util.IsEmptyBlock(&block) {
			anyBlockExecuted = true
		}

		blocks = append(blocks, &block)
	}

	var epochReceipts [][]types.TransactionReceipt
	if anyBlockExecuted && useBatch {
		// Batch get epoch receipts.
		epochReceipts, err = cfx.GetEpochReceiptsByPivotBlockHash(pivotHash)
		if checkPivotSwitchWithError(err) {
			logger.WithError(err).Info(
				"Failed to get epoch receipts with pivot assumption (regarded as pivot switch)",
			)

			err = ErrEpochPivotSwitched
		}

		if err != nil {
			return emptyEpochData, errors.WithMessagef(
				err, "failed to get epoch receipts by pivot %v", pivotHash,
			)
		}
	}

	receipts := make(map[types.Hash]*types.TransactionReceipt)

	for i, block := range blocks {
		var logIndex uint64 // block log index

		for j, tx := range block.Transactions {
			logger := logrus.WithFields(logrus.Fields{
				"i": i, "j": j,
				"epoch": epochNumber,
				"block": block.Hash.String(),
				"tx":    tx.Hash.String(),
			})

			// skip unexecuted transaction, e.g.
			// 1) already executed in previous block
			// 2) never executed, e.g. nonce mismatch
			if !util.IsTxExecutedInBlock(&tx) {
				logger.Debug("Transaction not executed in block")
				continue
			}

			var receipt *types.TransactionReceipt
			if useBatch {
				// If epoch not executed yet, cfx_getEpochReceipts will always return null no matter what block hash passed in
				// as assumptive pivot hash due to fullnode RPC implementation. While we have some executed transaction but
				// unexecuted receipt here, it is definitely resulted by pivot switch.
				if epochReceipts == nil {
					logger.Info("Failed to match tx receipts due to epoch receipts nil (regarded as pivot switch)")
					return emptyEpochData, errors.WithMessage(ErrEpochPivotSwitched, "batch retrieved epoch receipts nil")
				}

				// Find transaction receipts from above batch retrieved receipts.
				// The order of batch retrieved receipts should be the same with the fetched blocks & transactions,
				// Even so, we'd better also do some bound checking and fault tolerance to be robust.
				if i >= len(epochReceipts) || j >= len(epochReceipts[i]) {
					logger.WithField("epochReceipts", epochReceipts).Error("Batch retrieved receipts out of bound")
					return emptyEpochData, errors.New("batch retrieved receipts out of bound")
				}

				receipt = &epochReceipts[i][j]
				if receipt == nil {
					logger.Error("Batch retrieved receipt not found")
					return emptyEpochData, errors.Errorf("batch retrieved receipt not found for tx %v", tx.Hash)
				}
			} else {
				receipt, err = cfx.GetTransactionReceipt(tx.Hash)
				if err != nil {
					return emptyEpochData, errors.WithMessagef(err, "Failed to get receipt by tx hash %v", tx.Hash)
				}

				// While we have some executed transaction but unexecuted receipt here, it is definitely
				// resulted by pivot switch.
				if receipt == nil {
					logger.Info("Failed to get tx receipt due to receipt nil (regarded as pivot switch)")
					return emptyEpochData, errors.WithMessage(ErrEpochPivotSwitched, "retrieved tx receipt nil")
				}
			}

			if err := validateTxsReceipt(receipt, epochNumber, block, &tx); err != nil {
				logger.WithError(err).Info(
					"Failed to get transaction receipt (regarded as pivot switch)",
				)

				return emptyEpochData, errors.WithMessage(
					ErrEpochPivotSwitched, err.Error(),
				)
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

				if !IsAddressBlacklisted(&log.Address) { // skip blacklisted address
					logs = append(logs, log)
				}

				txLogIndex++
				logIndex++
			}
			receipt.Logs = logs // replace the origin logs

			receipts[tx.Hash] = receipt
		}
	}

	return EpochData{
		Number: epochNumber, Blocks: blocks, Receipts: receipts,
	}, nil
}

// Check if address blacklisted or not
func IsAddressBlacklisted(addr *cfxaddress.Address) bool {
	if len(blacklistedAddressSet) == 0 {
		return false
	}

	addrStr := addr.MustGetBase32Address()
	addrStr = strings.ToLower(addrStr)

	_, exists := blacklistedAddressSet[addrStr]
	return exists
}

func validateBlock(block *types.Block, epochNumber uint64, hash types.Hash) error {
	if block.EpochNumber == nil {
		return errors.WithMessage(errBlockValidationFailed, "epoch number is nil")
	}

	bEpochNumber := block.EpochNumber.ToInt().Uint64()
	if bEpochNumber != epochNumber {
		errMsg := fmt.Sprintf(
			"epoch number mismatched, expect %v got %v", epochNumber, bEpochNumber,
		)
		return errors.WithMessage(errBlockValidationFailed, errMsg)
	}

	if block.Hash != hash {
		return errors.WithMessage(errBlockValidationFailed, "block hash not matched")
	}

	return nil
}

func validateTxsReceipt(
	receipt *types.TransactionReceipt, epochNumber uint64,
	block *types.Block, tx *types.Transaction,
) error {
	// Check receipt epoch number
	rcptEpochNumber := uint64(*receipt.EpochNumber)
	if rcptEpochNumber != epochNumber {
		errMsg := fmt.Sprintf(
			"epoch number mismatched, expect %v got %v", epochNumber, rcptEpochNumber,
		)
		return errors.WithMessage(errTxsReceiptValidationFailed, errMsg)
	}

	// Check receipt block hash
	if receipt.BlockHash != block.Hash {
		errMsg := fmt.Sprintf(
			"block hash mismatched, expect %v got %v", block.Hash, receipt.BlockHash,
		)
		return errors.WithMessage(errTxsReceiptValidationFailed, errMsg)
	}

	// Check receipt transaction hash
	if receipt.TransactionHash != tx.Hash {
		errMsg := fmt.Sprintf(
			"txs hash mismatched, expect %v got %v", tx.Hash, receipt.TransactionHash,
		)
		return errors.WithMessage(errTxsReceiptValidationFailed, errMsg)
	}

	return nil
}

// Check if epoch pivot switched from query or validation error.
func checkPivotSwitchWithError(err error) bool {
	if err == nil {
		return false
	}

	// The error is an epoch block validation error, take it as pivot switched.
	if errors.Is(err, errBlockValidationFailed) {
		return true
	}

	// The error is detected as a business error and pivot hash assumption failed, must be pivot switched.
	detected, errCode := sdkerr.DetectErrorCode(err)
	if detected && (errCode == sdkerr.CodePivotAssumption || errCode == sdkerr.CodeBlockNotFound) {
		return true
	}

	return false
}
