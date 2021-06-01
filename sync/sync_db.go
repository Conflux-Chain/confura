package sync

import (
	"context"
	"sync"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	gometrics "github.com/ethereum/go-ethereum/metrics"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// DatabaseSyncer is used to sync blockchain data into database
// against the latest confirmed epoch.
type DatabaseSyncer struct {
	cfx                 sdk.ClientOperator
	db                  store.Store
	epochFrom           uint64        // epoch number to sync data from
	maxSyncEpochs       uint64        // maximum number of epochs to sync once
	syncIntervalNormal  time.Duration // interval to sync data in normal status
	syncIntervalCatchUp time.Duration // interval to sync data in catching up mode
	subEpochCh          chan uint64   // receive the epoch from pub/sub to detect pivot chain switch
	checkPointCh        chan bool     // checkpoint channel received to check sync data
}

// NewDatabaseSyncer creates an instance of DatabaseSyncer to sync blockchain data.
func NewDatabaseSyncer(cfx sdk.ClientOperator, db store.Store) *DatabaseSyncer {
	syncer := &DatabaseSyncer{
		cfx:                 cfx,
		db:                  db,
		epochFrom:           0,
		maxSyncEpochs:       viper.GetUint64("sync.maxEpochs"),
		syncIntervalNormal:  time.Second,
		syncIntervalCatchUp: time.Millisecond,
		subEpochCh:          make(chan uint64, viper.GetInt64("sync.sub.buffer")),
		checkPointCh:        make(chan bool, 2),
	}

	// Ensure confirmed sync epoch not reverted
	if err := ensureStoreEpochDataOk(cfx, db); err != nil {
		logrus.WithError(err).Fatal("failed to ensure last confirmed epoch data not reverted")
	}

	// Load last sync epoch information
	syncer.mustLoadLastSyncEpoch()

	return syncer
}

// Sync starts to sync epoch blockchain data with specified cfx instance.
func (syncer *DatabaseSyncer) Sync(ctx context.Context, wg *sync.WaitGroup) {
	logrus.WithField("epochFrom", syncer.epochFrom).Infof("DB sync starting to sync epoch data")

	wg.Add(1)
	defer wg.Done()

	ticker := time.NewTicker(syncer.syncIntervalCatchUp)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("DB syncer shutdown ok")
			return
		case <-syncer.checkPointCh:
			if err := syncer.doCheckPoint(); err != nil {
				logrus.WithError(err).Error("Failed to do sync checkpoint")
			}
		case newEpoch := <-syncer.subEpochCh:
			syncer.handleNewEpoch(newEpoch)
		case <-ticker.C:
			if err := syncer.doTicker(ticker); err != nil {
				logrus.WithError(err).WithField("epochFrom", syncer.epochFrom).Error("Failed to sync epoch data")
			}
		}
	}
}

// Load last sync epoch from databse to continue synchronization
func (syncer *DatabaseSyncer) mustLoadLastSyncEpoch() {
	_, maxEpoch, err := syncer.db.GetGlobalEpochRange()
	if err == nil {
		syncer.epochFrom = maxEpoch + 1
	} else if !syncer.db.IsRecordNotFound(err) {
		logrus.WithError(err).Fatal("Failed to read block epoch range from database")
	}
}

// Sync data once and return true if catch up to the latest confirmed epoch, otherwise false.
func (syncer *DatabaseSyncer) syncOnce() (bool, error) {
	// Fetch latest confirmed epoch info from blockchain
	epoch, err := syncer.cfx.GetEpochNumber(types.EpochLatestConfirmed)
	if err != nil {
		return false, errors.WithMessage(err, "Failed to query the latest confirmed epoch number")
	}

	updater := metrics.NewTimerUpdaterByName("infura/duration/db/sync/once")
	defer updater.Update()

	maxEpochTo := epoch.ToInt().Uint64()

	// already catch up to the latest confirmed epoch
	if syncer.epochFrom > maxEpochTo {
		logrus.WithFields(logrus.Fields{
			"epochRange": citypes.EpochRange{EpochFrom: syncer.epochFrom, EpochTo: maxEpochTo},
		}).Debug("DB sync skipped for invalid epoch range")

		return true, nil
	}

	// close to the latest confirmed epoch
	epochTo := syncer.epochFrom + syncer.maxSyncEpochs - 1
	if epochTo > maxEpochTo {
		epochTo = maxEpochTo
	}
	syncSize := epochTo - syncer.epochFrom + 1

	syncSizeGauge := gometrics.GetOrRegisterGauge("infura/db/sync/size/confirmed", nil)
	syncSizeGauge.Update(int64(syncSize))

	logger := logrus.WithFields(logrus.Fields{
		"syncSize":   syncSize,
		"epochRange": citypes.EpochRange{EpochFrom: syncer.epochFrom, EpochTo: epochTo},
	})
	logger.Debug("DB sync started to sync with epoch range")

	epochDataSlice := make([]*store.EpochData, 0, syncSize)

	for i := syncer.epochFrom; i <= epochTo; i++ {
		data, err := store.QueryEpochData(syncer.cfx, i)
		if err != nil {
			logrus.WithError(err).WithField("epoch", i).Error("Failed to query epoch data")

			return false, errors.WithMessagef(err, "Failed to query epoch data for epoch %v", i)
		}

		logrus.WithField("epoch", i).Debug("Succeeded to query epoch data")

		epochDataSlice = append(epochDataSlice, &data)
	}

	if err = syncer.db.Pushn(epochDataSlice); err != nil {
		logger.WithError(err).Error("Failed to write epoch data to database")

		return false, errors.WithMessage(err, "Failed to write epoch data to database")
	}

	logger.Trace("Succeeded to sync epoch data range")

	syncer.epochFrom = epochTo + 1

	return false, nil
}

func (syncer *DatabaseSyncer) doCheckPoint() error {
	logrus.Debug("DB sync doing checkpoint")

	// Try at most 50 times to ensure confirmed epoch data in db not reverted
	maxTries := 50
	for tryTimes := 0; tryTimes < maxTries; tryTimes++ {
		if err := ensureStoreEpochDataOk(syncer.cfx, syncer.db); err == nil {
			return nil
		} else if tryTimes == maxTries-1 {
			return err
		}
	}

	return nil
}

func (syncer *DatabaseSyncer) doTicker(ticker *time.Ticker) error {
	logrus.Debug("DB sync ticking")

	if complete, err := syncer.syncOnce(); err != nil {
		ticker.Reset(syncer.syncIntervalNormal)
		return err
	} else if complete {
		ticker.Reset(syncer.syncIntervalNormal)
	} else {
		ticker.Reset(syncer.syncIntervalCatchUp)
	}

	return nil
}

// implement the EpochSubscriber interface.
func (syncer *DatabaseSyncer) onEpochReceived(epoch types.WebsocketEpochResponse) {
	epochNo := epoch.EpochNumber.ToInt().Uint64()

	logrus.WithField("epoch", epochNo).Debug("DB sync onEpochReceived new epoch received")
	syncer.subEpochCh <- epochNo
}

func (syncer *DatabaseSyncer) onEpochSubStart() {
	logrus.Debug("DB sync onEpochSubStart event received")

	syncer.checkPointCh <- true
}

func (syncer *DatabaseSyncer) handleNewEpoch(newEpoch uint64) {
	if newEpoch >= syncer.epochFrom {
		return
	}

	// remove blockchain data from database due to pivot chain switch

	logger := logrus.WithFields(logrus.Fields{
		"epochFrom": newEpoch,
		"epochTo":   syncer.epochFrom - 1,
	})
	logger.Info("Begin to remove blockchain data due to pivot chain switch")

	// must ensure the reverted data removed from database
	for {
		err := syncer.db.Popn(newEpoch)
		if err == nil {
			// update syncer start epoch
			syncer.epochFrom = newEpoch
			break
		}

		logger.WithError(err).Error("Failed to remove blockchain data due to pivot chain switch")

		// retry after 5 seconds for any temp db issue
		time.Sleep(5 * time.Second)
	}

	logger.Info("Complete to remove blockchain data due to pivot chain switch")
}
