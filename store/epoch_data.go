package store

import (
	"fmt"
	"strings"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	sdkerr "github.com/Conflux-Chain/go-conflux-sdk/types/errors"
	"github.com/conflux-chain/conflux-infura/metrics"
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

	errBlockValidationFailed = errors.New("epoch block validation failed")
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
}

// QueryEpochData queries blockchain data for the specified epoch number.
// TODO better to use batch API to return all if performance is low in case of high TPS.
func QueryEpochData(cfx sdk.ClientOperator, epochNumber uint64) (EpochData, error) {
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

	validateBlock := func(block *types.Block) error { // epoch block validator
		if block.EpochNumber == nil {
			return errors.WithMessage(errBlockValidationFailed, "epoch number is nil")
		}

		bEpochNumber := block.EpochNumber.ToInt().Uint64()
		if bEpochNumber != epochNumber {
			errMsg := fmt.Sprintf("epoch number mismatched, expect %v got %v", epochNumber, bEpochNumber)
			return errors.WithMessage(errBlockValidationFailed, errMsg)
		}

		return nil
	}

	// Fetch epoch block summary one by one.
	for _, hash := range blockHashes {
		block, err := cfx.GetBlockByHashWithPivotAssumption(hash, pivotHash, hexutil.Uint64(epochNumber))
		if err == nil { // validate block first if no error
			err = validateBlock(&block)
		}

		if checkPivotSwitchWithError(err) { // check pivot switch
			l := logger.WithFields(logrus.Fields{"blockHash": hash, "blockHeader": &(block.BlockHeader)}).WithError(err)
			l.Info("Failed to get block by hash with pivot assumption (regarded as pivot switch)")

			err = ErrEpochPivotSwitched
		}

		if err != nil {
			return emptyEpochData, errors.WithMessagef(err, "failed to get block by hash %v", hash)
		}

		blocks = append(blocks, &block)
	}

	var epochReceipts [][]types.TransactionReceipt
	useBatch := viper.GetBool("sync.useBatch")

	if useBatch {
		// Batch get epoch receipts.
		epochReceipts, err = cfx.GetEpochReceiptsByPivotBlockHash(pivotHash)
		if checkPivotSwitchWithError(err) {
			logger.WithError(err).Info("Failed to get epoch receipts with pivot assumption (regarded as pivot switch)")

			err = ErrEpochPivotSwitched
		}

		if err != nil {
			return emptyEpochData, errors.WithMessagef(err, "failed to get epoch receipts by pivot %v", pivotHash)
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
			if tx.Status == nil || tx.BlockHash == nil {
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

				// While we have some executed transaction but unexecuted receipt here, it is definitely resulted by pivot switch.
				if receipt == nil {
					logger.Info("Failed to get tx receipt due to receipt nil (regarded as pivot switch)")
					return emptyEpochData, errors.WithMessage(ErrEpochPivotSwitched, "retrieved tx receipt nil")
				}
			}

			// Check receipt epoch number
			rcptEpochNumber := uint64(*receipt.EpochNumber)
			if rcptEpochNumber != epochNumber {
				logger.WithField("rcptEpochNumber", rcptEpochNumber).Error("Batch retrieved receipt epoch number mismatch")
				return emptyEpochData, errors.Errorf("batch retrieved receipt epoch number mismatch, value = %v", uint64(*receipt.EpochNumber))
			}

			// Check receipt block hash
			if receipt.BlockHash.String() != block.Hash.String() {
				logger.WithField("rcptBlockHash", receipt.BlockHash).Error("Batch retrieved receipt block hash mismatch")
				return emptyEpochData, errors.Errorf("batch retrieved receipt block hash mismatch, value = %v", receipt.BlockHash)
			}

			// Check receipt transaction hash
			if receipt.TransactionHash.String() != tx.Hash.String() {
				logger.WithField("rcptTxHash", receipt.TransactionHash).Error("Batched retrieved receipt transaction hash mismatch")
				return emptyEpochData, errors.Errorf("batched retrieved receipt transaction hash mismatch, value = %v", receipt.TransactionHash)
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

	return EpochData{epochNumber, blocks, receipts}, nil
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
	// TODO: uncomment the following to use the sdk method to detect pivot switch once relative issues
	// (https://github.com/Conflux-Chain/go-conflux-sdk/issues/118) resolved.
	// detected, errCode := sdkerr.DetectErrorCode(err)
	detected, errCode := detectErrorCode(err)
	if detected && (errCode == sdkerr.CodePivotAssumption || errCode == sdkerr.CodeBlockNotFound) {
		return true
	}

	return false
}

// TODO: remove this function once relative issues
// (https://github.com/Conflux-Chain/go-conflux-sdk/issues/118) resolved.
func detectErrorCode(err error) (ok bool, code sdkerr.ErrorCode) {
	ok, code = sdkerr.DetectErrorCode(err)
	if !ok && err != nil {
		errStr := strings.ToLower(err.Error())

		if strings.Contains(errStr, "pivot chain assumption failed") {
			return true, sdkerr.CodePivotAssumption
		}
	}

	return ok, code
}
