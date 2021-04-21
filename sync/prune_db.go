package sync

import (
	"time"

	"github.com/conflux-chain/conflux-infura/store"
	mstore "github.com/conflux-chain/conflux-infura/store/mysql"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type DBPruneConfig struct {
	PruneInterval  time.Duration    `mapstructure:"interval"`  // interval to run pruning (ms)
	Threshold      DBPruneThresHold `mapstructure:"threshold"` // threshold for pruning
	MaxPruneEpochs uint64           `mapstructure:"maxEpochs"` // max epochs to prune if threshold condition met
}

// Threshold settings for DB pruner
type DBPruneThresHold struct {
	MaxBlocks uint64 `mapstructure:"maxBlocks"` // max number of blocks to trigger block pruning
	MaxTxs    uint64 `mapstructure:"maxTxs"`    // max number of transactions to trigger transaction pruning
	MaxLogs   uint64 `mapstructure:"maxLogs"`   // max number of logs to trigger log pruning
}

// DBPruner is used to prune blockchain data in database periodly.
// It will prune blockchain data in db with epoch as the smallest unit to retain data atomicity.
type DBPruner struct {
	db          store.Store
	pruneConfig DBPruneConfig
}

// NewDBPruner creates an instance of DBPruner to prune blockchain data.
func NewDBPruner(db store.Store) *DBPruner {
	pruner := &DBPruner{db: db}
	if err := viper.Sub("prune.db").Unmarshal(&pruner.pruneConfig); err != nil {
		logrus.WithError(err).Fatal("DBPruner failed to load prune config")
	}

	return pruner
}

func (pruner *DBPruner) Prune() {
	ticker := time.NewTicker(pruner.pruneConfig.PruneInterval * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := pruner.doTicker(); err != nil {
				logrus.WithError(err).Error("DBPruner ticked error")
			}
		}
	}
}

func (pruner *DBPruner) doTicker() error {
	for _, dt := range mstore.OpEpochDataTypes {
		if err := pruner.pruneEpochData(dt); err != nil {
			logrus.WithError(err).Errorf("DB pruner failed to prune epoch %v", mstore.EpochDataTypeTableMap[dt])
			return err
		}
	}

	return nil
}

func (pruner *DBPruner) pruneEpochData(dt mstore.EpochDataType) error {
	var getNumEpochData func() uint64
	var threshold uint64

	switch dt {
	case mstore.EpochBlock:
		threshold = pruner.pruneConfig.Threshold.MaxBlocks
		getNumEpochData = pruner.db.GetNumBlocks
	case mstore.EpochTransaction:
		threshold = pruner.pruneConfig.Threshold.MaxTxs
		getNumEpochData = pruner.db.GetNumTransactions
	case mstore.EpochLog:
		threshold = pruner.pruneConfig.Threshold.MaxLogs
		getNumEpochData = pruner.db.GetNumLogs
	default:
		return errors.New("Unexpected epoch data type")
	}

	numData := getNumEpochData()

	for numData > threshold {
		logrus.WithFields(logrus.Fields{
			"numData": numData, "threshold": threshold,
		}).Debugf("DB pruner starting to prune epoch %v", mstore.EpochDataTypeTableMap[dt])

		if err := pruner.doPruneEpochData(dt); err != nil {
			return err
		}

		numData = getNumEpochData()
	}

	return nil
}

func (pruner *DBPruner) doPruneEpochData(dt mstore.EpochDataType) error {
	var getEpochRange func() (uint64, uint64, error)
	var dequeue func(uint64) error

	switch dt {
	case mstore.EpochBlock:
		dequeue = pruner.db.DequeueBlocks
		getEpochRange = pruner.db.GetBlockEpochRange
	case mstore.EpochTransaction:
		dequeue = pruner.db.DequeueTransactions
		getEpochRange = pruner.db.GetTransactionEpochRange
	case mstore.EpochLog:
		dequeue = pruner.db.DequeueLogs
		getEpochRange = pruner.db.GetLogEpochRange
	default:
		return errors.New("Unexpected epoch data type")
	}

	// Get epoch range
	minEpoch, maxEpoch, err := getEpochRange()
	if err != nil {
		werr := errors.WithMessagef(err, "Failed to get %v epoch range", mstore.EpochDataTypeTableMap[dt])
		return werr
	}

	// Calculate max epoch number until to which epoch data to be dequeued
	epochUntil := minEpoch + pruner.pruneConfig.MaxPruneEpochs - 1
	epochUntil = util.MinUint64(epochUntil, maxEpoch)

	logrus.WithField("epochUntil", epochUntil).Debugf("DB pruner dequeue epoch %v data", mstore.EpochDataTypeTableMap[dt])

	// Dequeue epoch data
	if err := dequeue(epochUntil); err != nil {
		werr := errors.WithMessagef(err, "Failed to dequeue epoch %v", mstore.EpochDataTypeTableMap[dt])
		return werr
	}

	return nil
}
