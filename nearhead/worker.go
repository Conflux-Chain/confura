package nearhead

import (
	"math/big"
	"sync"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/util/metrics"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	lock              sync.RWMutex
	cachedEpoch       LatestEpochData
	statedBlockWindow = newBlockWindow(viper.GetUint64("nearhead.cap"))

	cacheTimer    = metrics.GetOrRegisterTimer("infura/nearhead/cache/stated")
	epochGapGauge = metrics.GetOrRegisterGauge("infura/nearhead/epoch/gap/stated")
)

// Start starts to cache near head data, e.g. blocks, transactions and receipts.
func Start(cfx sdk.ClientOperator, statedEpochCh <-chan *big.Int) {
	for epoch := range statedEpochCh {
		// TODO handle RPC error
		start := time.Now()
		if err := doCache(cfx, epoch); err == nil {
			cacheTimer.UpdateSince(start)

			if d := time.Since(start); d > time.Millisecond*3 {
				logrus.Warnf("It takes too long to cache data, duration = %v", d)
			}
		}
	}
}

func doCache(cfx sdk.ClientOperator, statedEpoch *big.Int) error {
	statedEpochNum := statedEpoch.Uint64()
	logger := logrus.WithField("statedEpoch", statedEpochNum)

	// epoch gap statistics
	if num, err := cfx.GetEpochNumber(types.EpochLatestState); err == nil {
		gap := num.ToInt().Int64() - statedEpoch.Int64()
		epochGapGauge.Update(gap)
		if gap < -1 || gap > 1 {
			logger.WithField("gap", gap).Trace("epoch gap fall behind")
		}
	}

	epochData, err := newLatestEpochDataWithStated(cfx, statedEpoch)
	if err != nil {
		logger.WithError(err).Error("Failed to get latest epoch data")
		return err
	}

	blocks, receipts, err := getExecutedBlocks(cfx, statedEpoch)
	if err != nil {
		logger.WithError(err).Error("Failed to get blocks for epoch")
		return err
	}

	lock.Lock()
	defer lock.Unlock()

	cachedEpoch = epochData

	// handle re-org for the latest state epoch
	for statedBlockWindow.maxEpoch > 0 && statedBlockWindow.maxEpoch >= statedEpochNum {
		logger = logger.WithField("windowMaxEpoch", statedBlockWindow.maxEpoch)

		if statedBlockWindow.pop() {
			logger.Info("Succeed to revert epoch for stated block window")
		} else {
			logger.Warn("Cannot revert epoch for empty stated block window")
		}
	}

	if !statedBlockWindow.push(statedEpochNum, blocks, receipts) {
		logger.Error("Failed to cache data for stated block window")
	}

	logger.Debug("Succeed to cache data for stated block window")

	return nil
}

// GetLatestEpochData returns the cached epoch data.
func GetLatestEpochData() LatestEpochData {
	lock.RLock()
	defer lock.RUnlock()

	return cachedEpoch
}

// GetBlockByHash returns the cached block for the specified hash if any.
func GetBlockByHash(blockHash string) (*types.Block, bool) {
	lock.RLock()
	defer lock.RUnlock()

	block, ok := statedBlockWindow.hash2Blocks[blockHash]

	return block, ok
}

// GetBlockByEpoch returns the cached block for the specified epoch if any.
func GetBlockByEpoch(epoch *types.Epoch) (*types.Block, bool) {
	lock.RLock()
	defer lock.RUnlock()

	epochNumber, ok := cachedEpoch.ToInt(epoch)
	if !ok {
		return nil, false
	}

	blocks, ok := statedBlockWindow.epoch2Blocks[epochNumber.Uint64()]
	if !ok {
		return nil, false
	}

	return blocks[len(blocks)-1], true
}

// GetBlocksByEpoch returns the cached blocks for the specified epoch if any.
func GetBlocksByEpoch(epoch *types.Epoch) ([]types.Hash, bool) {
	lock.RLock()
	defer lock.RUnlock()

	epochNumber, ok := cachedEpoch.ToInt(epoch)
	if !ok {
		return nil, false
	}

	blocks, ok := statedBlockWindow.epoch2Blocks[epochNumber.Uint64()]
	if !ok {
		return nil, false
	}

	var hashes []types.Hash
	for _, b := range blocks {
		hashes = append(hashes, b.Hash)
	}

	return hashes, true
}

// GetTransactionByHash returns the cached transaction for specified hash if any.
func GetTransactionByHash(txHash string) (*types.Transaction, bool) {
	lock.RLock()
	defer lock.RUnlock()

	if sharedTx, ok := statedBlockWindow.hash2Txs[txHash]; ok {
		return sharedTx.tx, ok
	}

	return nil, false
}

// GetReceiptByHash returns the cached transaction receipt for specified hash if any.
func GetReceiptByHash(txHash string) (*types.TransactionReceipt, bool) {
	lock.RLock()
	defer lock.RUnlock()

	if sharedTx, ok := statedBlockWindow.hash2Txs[txHash]; ok {
		return sharedTx.receipt, ok
	}

	return nil, false
}

// GetLogs returns the cached logs if any.
func GetLogs(filter types.LogFilter) ([]types.Log, bool) {
	lock.RLock()
	defer lock.RUnlock()

	epochFrom, ok := cachedEpoch.ToInt(filter.FromEpoch)
	if !ok {
		return nil, false
	}

	epochTo, ok := cachedEpoch.ToInt(filter.ToEpoch)
	if !ok {
		return nil, false
	}

	logFilter := newLogFilter(epochFrom.Uint64(), epochTo.Uint64(), &filter)

	return statedBlockWindow.getLogs(logFilter)
}
