package sync

import (
	"context"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/catchup"
	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// default capacity setting for pivot info window
	syncPivotInfoWinCapacity = 50
)

// db sync configuration
type syncConfig struct {
	FromEpoch uint64 `default:"0"`
	MaxEpochs uint64 `default:"10"`
	UseBatch  bool   `default:"false"`
	Sub       syncSubConfig
}

type syncSubConfig struct {
	Buffer uint64 `default:"1000"`
}

// DatabaseSyncer is used to sync blockchain data into database
// against the latest confirmed epoch.
type DatabaseSyncer struct {
	conf *syncConfig
	// conflux sdk client
	cfx sdk.ClientOperator
	// db store
	db *mysql.MysqlStore
	// epoch number to sync data from
	epochFrom uint64
	// maximum number of epochs to sync once
	maxSyncEpochs uint64
	// interval to sync data in normal status
	syncIntervalNormal time.Duration
	// interval to sync data in catching up mode
	syncIntervalCatchUp time.Duration
	// window to cache epoch pivot info
	epochPivotWin *epochPivotWindow
	// error tolerant logger
	etLogger *logutil.ErrorTolerantLogger
}

// MustNewDatabaseSyncer creates an instance of DatabaseSyncer to sync blockchain data.
func MustNewDatabaseSyncer(cfx sdk.ClientOperator, db *mysql.MysqlStore) *DatabaseSyncer {
	var conf syncConfig
	viperutil.MustUnmarshalKey("sync", &conf)

	syncer := &DatabaseSyncer{
		conf:                &conf,
		cfx:                 cfx,
		db:                  db,
		epochFrom:           0,
		maxSyncEpochs:       conf.MaxEpochs,
		syncIntervalNormal:  time.Second,
		syncIntervalCatchUp: time.Millisecond,
		epochPivotWin:       newEpochPivotWindow(syncPivotInfoWinCapacity),
		etLogger:            logutil.NewErrorTolerantLogger(logutil.DefaultETConfig),
	}

	// Ensure epoch data validity in database
	if err := ensureStoreEpochDataOk(cfx, db); err != nil {
		logrus.WithError(err).Fatal("Db sync failed to ensure epoch data validity in db")
	}

	// Load last sync epoch information
	syncer.mustLoadLastSyncEpoch()

	return syncer
}

// Sync starts to sync epoch blockchain data.
func (syncer *DatabaseSyncer) Sync(ctx context.Context, wg *sync.WaitGroup) {
	logrus.WithField("epochFrom", syncer.epochFrom).Info("DB sync starting to sync epoch data")

	wg.Add(1)
	defer wg.Done()

	syncer.fastCatchup(ctx)

	ticker := time.NewTicker(syncer.syncIntervalCatchUp)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("DB syncer shutdown ok")
			return
		case <-ticker.C:
			err := syncer.doTicker(ticker)
			syncer.etLogger.Log(
				logrus.WithField("epochFrom", syncer.epochFrom),
				err, "Db syncer failed to sync epoch data",
			)
		}
	}
}

// fast catch-up until the latest stable epoch
// (maximum between the latest finalized and checkpoint epoch)
func (syncer *DatabaseSyncer) fastCatchup(ctx context.Context) {
	catchUpSyncer := catchup.MustNewSyncer(
		syncer.cfx, syncer.db, catchup.WithEpochFrom(syncer.epochFrom),
	)
	defer catchUpSyncer.Close()

	catchUpSyncer.Sync(ctx)

	// start to sync from new start epoch after fast catch-up
	syncer.epochFrom = catchUpSyncer.Range().From
}

// Load last sync epoch from databse to continue synchronization.
func (syncer *DatabaseSyncer) mustLoadLastSyncEpoch() {
	loaded, err := syncer.loadLastSyncEpoch()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to load last sync epoch range from db")
	}

	// Load db sync start epoch config on initial loading if necessary.
	if !loaded && syncer.conf != nil {
		syncer.epochFrom = syncer.conf.FromEpoch
	}
}

func (syncer *DatabaseSyncer) loadLastSyncEpoch() (loaded bool, err error) {
	maxEpoch, ok, err := syncer.db.MaxEpoch()
	if err != nil {
		return false, errors.WithMessage(err, "failed to get max epoch from epoch to block mapping")
	}

	if ok {
		syncer.epochFrom = maxEpoch + 1
	}

	return ok, nil
}

// Sync data once and return true if catch up to the latest confirmed epoch, otherwise false.
func (syncer *DatabaseSyncer) syncOnce() (bool, error) {
	// Fetch latest confirmed epoch from blockchain
	epoch, err := syncer.cfx.GetEpochNumber(types.EpochLatestConfirmed)
	if err != nil {
		return false, errors.WithMessage(
			err, "failed to query the latest confirmed epoch number",
		)
	}

	maxEpochTo := epoch.ToInt().Uint64()
	if syncer.epochFrom > maxEpochTo { // cached up to the latest confirmed epoch?
		logrus.WithField("epochRange", citypes.RangeUint64{
			From: syncer.epochFrom,
			To:   maxEpochTo,
		}).Debug("Db syncer skipped due to already catch-up")
		return true, nil
	}

	epochTo, syncSize := syncer.nextEpochTo(maxEpochTo)

	logger := logrus.WithFields(logrus.Fields{
		"syncSize":       syncSize,
		"syncEpochRange": citypes.RangeUint64{From: syncer.epochFrom, To: epochTo},
	})
	logger.Debug("DB sync started to sync with epoch range")

	epochDataSlice := make([]*store.EpochData, 0, syncSize)
	for i := uint64(0); i < syncSize; i++ {
		epochNo := syncer.epochFrom + uint64(i)
		eplogger := logger.WithField("epoch", epochNo)

		data, err := store.QueryEpochData(syncer.cfx, epochNo, syncer.conf.UseBatch)

		// If epoch pivot chain switched, stop the querying right now since it's pointless to query epoch data
		// that will be reverted late.
		if errors.Is(err, store.ErrEpochPivotSwitched) {
			eplogger.WithError(err).Info("Db syncer failed to query epoch data due to pivot switch")
			break
		}

		if err != nil {
			return false, errors.WithMessagef(err, "failed to query epoch data for epoch %v", epochNo)
		}

		if i == 0 { // the first epoch must be continuous to the latest epoch in db store
			latestPivotHash, err := syncer.getStoreLatestPivotHash()
			if err != nil {
				eplogger.WithError(err).Error(
					"Db syncer failed to get latest pivot hash from db for parent hash check",
				)
				return false, errors.WithMessage(err, "failed to get latest pivot hash")
			}

			if len(latestPivotHash) > 0 && data.GetPivotBlock().ParentHash != latestPivotHash {
				latestStoreEpochNo := syncer.latestStoreEpoch()
				eplogger.WithFields(logrus.Fields{
					"latestStoreEpoch": latestStoreEpochNo,
					"latestPivotHash":  latestPivotHash,
				}).Warn("Db syncer popping latest epoch from db store due to parent hash mismatched")

				if err := syncer.pivotSwitchRevert(latestStoreEpochNo); err != nil {
					eplogger.WithError(err).Error(
						"Db syncer failed to pop latest epoch from db store due to parent hash mismatched",
					)

					return false, errors.WithMessage(
						err, "failed to pop latest epoch from db store due to parent hash mismatched",
					)
				}

				return false, nil
			}
		} else { // otherwise non-first epoch must also be continuous to previous one
			continuous, desc := data.IsContinuousTo(epochDataSlice[i-1])
			if !continuous {
				// truncate the batch synced epoch data until the previous epoch
				epochDataSlice = epochDataSlice[:i-1]

				eplogger.WithField("i", i).Infof(
					"Db syncer truncated batch synced data due to epoch not continuous for %v", desc,
				)
				break
			}
		}

		epochDataSlice = append(epochDataSlice, &data)

		eplogger.Debug("Db syncer succeeded to query epoch data")
	}

	metrics.Registry.Sync.SyncOnceSize("cfx", "db").Update(int64(len(epochDataSlice)))

	if len(epochDataSlice) == 0 { // empty epoch data query
		logger.Debug("Db syncer skipped due to empty sync range")
		return false, nil
	}

	if err = syncer.db.Pushn(epochDataSlice); err != nil {
		logger.WithError(err).Error("Db syncer failed to save epoch data to db")
		return false, errors.WithMessage(err, "failed to save epoch data to db")
	}

	syncer.epochFrom += uint64(len(epochDataSlice))

	for _, epdata := range epochDataSlice { // cache epoch pivot info for late use
		err := syncer.epochPivotWin.push(epdata.GetPivotBlock())
		if err != nil {
			logger.WithField("pivotBlockEpoch", epdata.Number).WithError(err).Info(
				"Db syncer failed to push pivot block into epoch cache window",
			)

			syncer.epochPivotWin.reset()
			break
		}
	}

	logger.WithFields(logrus.Fields{
		"newSyncFrom":   syncer.epochFrom,
		"finalSyncSize": len(epochDataSlice),
	}).Debug("Db syncer succeeded to sync epoch data range")

	return false, nil
}

func (syncer *DatabaseSyncer) doTicker(ticker *time.Ticker) error {
	logrus.Debug("DB sync ticking")

	start := time.Now()
	complete, err := syncer.syncOnce()
	metrics.Registry.Sync.SyncOnceQps("cfx", "db", err).UpdateSince(start)

	if err != nil {
		ticker.Reset(syncer.syncIntervalNormal)
		return err
	} else if complete {
		ticker.Reset(syncer.syncIntervalNormal)
	} else {
		ticker.Reset(syncer.syncIntervalCatchUp)
	}

	return nil
}

func (syncer *DatabaseSyncer) nextEpochTo(maxEpochTo uint64) (uint64, uint64) {
	epochTo := util.MinUint64(syncer.epochFrom+syncer.maxSyncEpochs-1, maxEpochTo)

	if epochTo < syncer.epochFrom {
		return epochTo, 0
	}

	syncSize := epochTo - syncer.epochFrom + 1
	return epochTo, syncSize
}

func (syncer *DatabaseSyncer) pivotSwitchRevert(revertTo uint64) error {
	if revertTo == 0 {
		return errors.New("genesis epoch must not be reverted")
	}

	logger := logrus.WithFields(logrus.Fields{
		"revertToEpoch":    revertTo,
		"latestStoreEpoch": syncer.latestStoreEpoch(),
	})

	if revertTo >= syncer.epochFrom {
		logger.Debug(
			"Db syncer skipped pivot switch revert due to not catched up yet",
		)
		return nil
	}

	logger.Info("Db syncer reverting epoch data due to pivot chain switch")

	// remove epoch data from database due to pivot switch
	if err := syncer.db.Popn(revertTo); err != nil {
		logger.WithError(err).Error(
			"Db syncer failed to pop epoch data from db due to pivot switch",
		)

		return errors.WithMessage(err, "failed to pop epoch data from db")
	}

	// remove pivot data of reverted epoch from cache window
	syncer.epochPivotWin.popn(revertTo)
	// update syncer start epoch
	syncer.epochFrom = revertTo

	return nil
}

func (syncer *DatabaseSyncer) getStoreLatestPivotHash() (types.Hash, error) {
	if syncer.epochFrom == 0 { // no epoch synchronized yet
		return "", nil
	}

	latestEpochNo := syncer.latestStoreEpoch()

	// load from in-memory cache first
	if pivotHash, ok := syncer.epochPivotWin.getPivotHash(latestEpochNo); ok {
		return pivotHash, nil
	}

	pivotHash, _, err := syncer.db.PivotHash(latestEpochNo)
	return types.Hash(pivotHash), err
}

func (syncer *DatabaseSyncer) latestStoreEpoch() uint64 {
	if syncer.epochFrom > 0 {
		return syncer.epochFrom - 1
	}

	return 0
}
