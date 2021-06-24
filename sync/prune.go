package sync

import (
	"context"
	"sync"
	"time"

	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	unexpectedEpochDataType = errors.New("Unexpected epoch data type")
)

type PruneConfig struct {
	PruneInterval  time.Duration  `mapstructure:"interval"`  // interval to run pruning
	Threshold      PruneThresHold `mapstructure:"threshold"` // threshold for pruning
	MaxPruneEpochs uint64         `mapstructure:"maxEpochs"` // max epochs to prune if threshold condition met
}

// Threshold settings for DB pruner
type PruneThresHold struct {
	MaxBlocks uint64 `mapstructure:"maxBlocks"` // max number of blocks to trigger block pruning
	MaxTxs    uint64 `mapstructure:"maxTxs"`    // max number of transactions to trigger transaction pruning
	MaxLogs   uint64 `mapstructure:"maxLogs"`   // max number of logs to trigger log pruning
}

// MustNewDBPruner creates an instance of Pruner to prune blockchain data in database
func MustNewDBPruner(db store.Store) *Pruner {
	var pc PruneConfig
	if err := viper.Sub("prune.db").Unmarshal(&pc); err != nil {
		logrus.WithError(err).Fatal("DBPruner failed to load prune config")
	}

	return newPruner(db, &pc)
}

// MustNewKVCachePruner creates an instance of Pruner to prune blockchain data in kv cache
func MustNewKVCachePruner(cache store.Store) *Pruner {
	var pc PruneConfig
	if err := viper.Sub("prune.cache").Unmarshal(&pc); err != nil {
		logrus.WithError(err).Fatal("KVPruner failed to load prune config")
	}

	return newPruner(cache, &pc)
}

// Pruner is used to prune blockchain data in store periodly.
// It will prune blockchain data in store with epoch as the smallest unit to retain data atomicity.
type Pruner struct {
	store       store.Store
	pruneConfig *PruneConfig
}

// newPruner creates an instance of Pruner to prune blockchain data.
func newPruner(store store.Store, pc *PruneConfig) *Pruner {
	return &Pruner{store: store, pruneConfig: pc}
}

func (pruner *Pruner) Prune(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	ticker := time.NewTicker(pruner.pruneConfig.PruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Infof("Pruner shutdown ok")
			return
		case <-ticker.C:
			if err := pruner.doTicker(); err != nil {
				logrus.WithError(err).Errorf("Pruner ticked error")
			}
		}
	}
}

func (pruner *Pruner) doTicker() error {
	for _, dt := range store.OpEpochDataTypes {
		if err := pruner.pruneEpochData(dt); err != nil {
			logrus.WithError(err).Errorf("Pruner failed to prune epoch %v", store.EpochDataTypeToStr(dt))
			return err
		}
	}

	return nil
}

func (pruner *Pruner) pruneEpochData(dt store.EpochDataType) error {
	var getNumEpochData func() uint64
	var threshold uint64

	switch dt {
	case store.EpochBlock:
		threshold = pruner.pruneConfig.Threshold.MaxBlocks
		getNumEpochData = pruner.store.GetNumBlocks
	case store.EpochTransaction:
		threshold = pruner.pruneConfig.Threshold.MaxTxs
		getNumEpochData = pruner.store.GetNumTransactions
	case store.EpochLog:
		threshold = pruner.pruneConfig.Threshold.MaxLogs
		getNumEpochData = pruner.store.GetNumLogs
	default:
		return unexpectedEpochDataType
	}

	numData := getNumEpochData()

	for numData > threshold {
		logrus.WithFields(logrus.Fields{
			"numData": numData, "threshold": threshold,
		}).Infof("Pruner starting to prune epoch %v", store.EpochDataTypeToStr(dt))

		if err := pruner.doPruneEpochData(dt); err != nil {
			return err
		}

		numData = getNumEpochData()
	}

	return nil
}

func (pruner *Pruner) doPruneEpochData(dt store.EpochDataType) error {
	var getEpochRange func() (uint64, uint64, error)
	var dequeue func(uint64) error

	switch dt {
	case store.EpochBlock:
		dequeue = pruner.store.DequeueBlocks
		getEpochRange = pruner.store.GetBlockEpochRange
	case store.EpochTransaction:
		dequeue = pruner.store.DequeueTransactions
		getEpochRange = pruner.store.GetTransactionEpochRange
	case store.EpochLog:
		dequeue = pruner.store.DequeueLogs
		getEpochRange = pruner.store.GetLogEpochRange
	default:
		return unexpectedEpochDataType
	}

	// Get epoch range
	minEpoch, maxEpoch, err := getEpochRange()
	if err != nil {
		werr := errors.WithMessagef(err, "failed to get %v epoch range", store.EpochDataTypeToStr(dt))
		return werr
	}

	// Calculate max epoch number until to which epoch data to be dequeued
	epochUntil := minEpoch + pruner.pruneConfig.MaxPruneEpochs - 1
	epochUntil = util.MinUint64(epochUntil, maxEpoch)

	logrus.WithField("epochUntil", epochUntil).Infof("DB pruner dequeue epoch %v data", store.EpochDataTypeToStr(dt))

	// Dequeue epoch data
	if err := dequeue(epochUntil); err != nil {
		werr := errors.WithMessagef(err, "failed to dequeue epoch %v", store.EpochDataTypeToStr(dt))
		return werr
	}

	return nil
}
