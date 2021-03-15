package sync

import (
	"math/big"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// DatabaseSyncer is used to sync blockchain data into database
// against the latest confirmed epoch.
type DatabaseSyncer struct {
	cfx                 sdk.ClientOperator
	db                  store.Store
	epochFrom           int64         // epoch number to sync data from
	maxSyncEpochs       int64         // maximum number of epochs to sync once
	syncIntervalNormal  time.Duration // interval to sync data in normal status
	syncIntervalCatchUp time.Duration // interval to sync data in catching up mode
	subEpochCh          chan int64    // receive the epoch from pub/sub to detect pivot chain switch
}

// NewDatabaseSyncer creates an instance of DatabaseSyncer to sync blockchain data.
func NewDatabaseSyncer(cfx sdk.ClientOperator, db store.Store) *DatabaseSyncer {
	return &DatabaseSyncer{
		cfx:                 cfx,
		db:                  db,
		epochFrom:           0,
		maxSyncEpochs:       viper.GetInt64("sync.maxEpochs"),
		syncIntervalNormal:  time.Second,
		syncIntervalCatchUp: time.Millisecond,
		subEpochCh:          make(chan int64, viper.GetInt64("sync.sub.buffer")),
	}
}

// Sync starts to sync epoch blockchain data with specified cfx instance.
func (syncer *DatabaseSyncer) Sync() {
	syncer.mustLoadLastSyncEpoch()

	ticker := time.NewTicker(syncer.syncIntervalCatchUp)
	defer ticker.Stop()

	for {
		select {
		case newEpoch := <-syncer.subEpochCh:
			syncer.handleNewEpoch(newEpoch)
		case <-ticker.C:
			complete, err := syncer.syncOnce()
			if err != nil {
				logrus.WithError(err).WithField("epochFrom", syncer.epochFrom).Error("Failed to sync epoch data")
				ticker.Reset(syncer.syncIntervalNormal)
			} else if complete {
				ticker.Reset(syncer.syncIntervalNormal)
			} else {
				ticker.Reset(syncer.syncIntervalCatchUp)
			}
		}
	}
}

// Load last sync epoch from databse to continue synchronization
func (syncer *DatabaseSyncer) mustLoadLastSyncEpoch() {
	minEpoch, maxEpoch, err := syncer.db.GetBlockEpochRange()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to read block epoch range from database")
	}

	logrus.WithFields(logrus.Fields{
		"minEpoch": minEpoch,
		"maxEpoch": maxEpoch,
	}).Info("Start to sync epoch data to database")

	if maxEpoch != nil {
		syncer.epochFrom = maxEpoch.Int64() + 1
	}
}

// Sync data once and return true if catch up to the latest confirmed epoch, otherwise false.
func (syncer *DatabaseSyncer) syncOnce() (bool, error) {
	updater := metrics.NewTimerUpdaterByName("infura/sync/once")
	defer updater.Update()

	epoch, err := syncer.cfx.GetEpochNumber(types.EpochLatestConfirmed)
	if err != nil {
		return false, errors.WithMessage(err, "Failed to query the latest confirmed epoch number")
	}

	maxEpochTo := epoch.ToInt().Int64()

	// already catch up to the latest confirmed epoch
	if syncer.epochFrom > maxEpochTo {
		return true, nil
	}

	// close to the latest confirmed epoch
	epochTo := syncer.epochFrom + syncer.maxSyncEpochs - 1
	if epochTo > maxEpochTo {
		epochTo = maxEpochTo
	}

	epochDataSlice := make([]*store.EpochData, 0, epochTo-syncer.epochFrom+1)

	for i := syncer.epochFrom; i <= epochTo; i++ {
		data, err := store.QueryEpochData(syncer.cfx, big.NewInt(i))
		if err != nil {
			return false, errors.WithMessagef(err, "Failed to query epoch data for epoch %v", i)
		}

		epochDataSlice = append(epochDataSlice, &data)
	}

	if err = syncer.db.PutEpochDataSlice(epochDataSlice); err != nil {
		return false, errors.WithMessage(err, "Failed to write epoch data to database")
	}

	syncer.epochFrom = epochTo + 1

	return false, nil
}

// implement the EpochSubscriber interface.
func (syncer *DatabaseSyncer) onEpochReceived(epoch types.WebsocketEpochResponse) {
	syncer.subEpochCh <- epoch.EpochNumber.ToInt().Int64()
}

func (syncer *DatabaseSyncer) handleNewEpoch(newEpoch int64) {
	if newEpoch >= syncer.epochFrom {
		return
	}

	// remove blockchain data from database due to pivot chain switch

	epochFrom := big.NewInt(newEpoch)
	epochTo := big.NewInt(syncer.epochFrom - 1)

	logger := logrus.WithFields(logrus.Fields{
		"epochFrom": epochFrom,
		"epochTo":   epochTo,
	})

	logger.Info("Begin to remove blockchain data due to pivot chain switch")

	// must ensure the reverted data removed from database
	for {
		err := syncer.db.Remove(epochFrom, epochTo)
		if err == nil {
			break
		}

		logger.WithError(err).Error("Failed to remove blockchain data due to pivot chain switch")

		// retry after 5 seconds for any temp db issue
		time.Sleep(5 * time.Second)
	}

	logger.Info("Complete to remove blockchain data due to pivot chain switch")
}
