package sync

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/catchup"
	"github.com/Conflux-Chain/confura/sync/election"
	"github.com/Conflux-Chain/confura/sync/monitor"
	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/dlock"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
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
	// conflux sdk clients
	cfxs []*sdk.Client
	// selected sdk client index
	cfxIdx atomic.Uint32
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
	// HA leader/follower election manager
	elm election.LeaderManager
	// sync monitor
	monitor *monitor.Monitor
}

// MustNewDatabaseSyncer creates an instance of DatabaseSyncer to sync blockchain data.
func MustNewDatabaseSyncer(cfxClients []*sdk.Client, db *mysql.MysqlStore) *DatabaseSyncer {
	if len(cfxClients) == 0 {
		logrus.Fatal("No sdk client provided")
	}

	var conf syncConfig
	viperutil.MustUnmarshalKey("sync", &conf)

	dlm := dlock.NewLockManager(dlock.NewMySQLBackend(db.DB()))
	monitor := monitor.NewMonitor(monitor.NewConfig(), func() (latestEpochNum uint64, retErr error) {
		for _, cfx := range cfxClients {
			epoch, err := cfx.GetEpochNumber(types.EpochLatestConfirmed)
			if err == nil {
				latestEpochNum = max(latestEpochNum, epoch.ToInt().Uint64())
			} else {
				retErr = err
			}
		}
		if latestEpochNum > 0 {
			return latestEpochNum, nil
		}
		return 0, retErr
	})

	syncer := &DatabaseSyncer{
		conf:                &conf,
		cfxs:                cfxClients,
		db:                  db,
		epochFrom:           0,
		maxSyncEpochs:       conf.MaxEpochs,
		syncIntervalNormal:  time.Second,
		syncIntervalCatchUp: time.Millisecond,
		monitor:             monitor,
		epochPivotWin:       newEpochPivotWindow(syncPivotInfoWinCapacity),
		elm:                 election.MustNewLeaderManagerFromViper(dlm, "sync.cfx"),
	}
	monitor.SetObserver(syncer)

	// Register leader election callbacks
	syncer.elm.OnElected(func(ctx context.Context, lm election.LeaderManager) {
		syncer.monitor.Start(ctx)
		syncer.onLeadershipChanged(ctx, lm, true)
	})

	syncer.elm.OnOusted(func(ctx context.Context, lm election.LeaderManager) {
		syncer.monitor.Stop()
		syncer.onLeadershipChanged(ctx, lm, false)
	})

	// Ensure epoch data validity in database
	if err := ensureStoreEpochDataOk(cfxClients[0], db); err != nil {
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

	go syncer.elm.Campaign(ctx)
	defer syncer.elm.Stop()

	syncer.fastCatchup(ctx)

	ticker := time.NewTimer(syncer.syncIntervalCatchUp)
	defer ticker.Stop()

	etLogger := logutil.NewErrorTolerantLogger(logutil.DefaultETConfig)
	defer logrus.Info("DB syncer shutdown ok")

	for syncer.elm.Await(ctx) {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := syncer.doTicker(ctx, ticker)
			etLogger.Log(
				logrus.WithField("epochFrom", syncer.epochFrom),
				err, "Db syncer failed to sync epoch data",
			)
		}
	}
}

// fast catch-up until the latest stable epoch
// (maximum between the latest finalized and checkpoint epoch)
func (syncer *DatabaseSyncer) fastCatchup(ctx context.Context) {
	catchUpSyncer := catchup.MustNewSyncer(syncer.cfxs, syncer.db, syncer.elm)
	defer catchUpSyncer.Close()

	catchUpSyncer.Sync(ctx)
}

// Load last sync epoch from databse to continue synchronization.
func (syncer *DatabaseSyncer) mustLoadLastSyncEpoch() {
	if err := syncer.loadLastSyncEpoch(); err != nil {
		logrus.WithError(err).Fatal("Failed to load last sync epoch range from db")
	}
}

func (syncer *DatabaseSyncer) loadLastSyncEpoch() error {
	// Load last sync epoch from databse
	maxEpoch, ok, err := syncer.db.MaxEpoch()
	if err != nil {
		return errors.WithMessage(err, "failed to get max epoch from epoch to block mapping")
	}

	if ok { // continue from the last sync epoch
		syncer.epochFrom = maxEpoch + 1
	} else { // start from genesis or configured start epoch
		syncer.epochFrom = 0
		if syncer.conf != nil {
			syncer.epochFrom = syncer.conf.FromEpoch
		}
	}

	return nil
}

// Sync data once and return true if catch up to the latest confirmed epoch, otherwise false.
func (syncer *DatabaseSyncer) syncOnce(ctx context.Context) (bool, error) {
	cfx := syncer.cfxs[syncer.cfxIdx.Load()]

	// Fetch latest confirmed epoch from blockchain
	epoch, err := cfx.GetEpochNumber(types.EpochLatestConfirmed)
	if err != nil {
		return false, errors.WithMessage(
			err, "failed to query the latest confirmed epoch number",
		)
	}

	// Load latest sync epoch from database
	if err := syncer.loadLastSyncEpoch(); err != nil {
		return false, errors.WithMessage(err, "failed to load last sync epoch")
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

		data, err := store.QueryEpochData(cfx, epochNo, syncer.conf.UseBatch)

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

				if err := syncer.pivotSwitchRevert(ctx, latestStoreEpochNo); err != nil {
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

	err = syncer.db.PushnWithFinalizer(epochDataSlice, func(d *gorm.DB) error {
		return syncer.elm.Extend(ctx)
	})

	if err != nil {
		if errors.Is(err, store.ErrLeaderRenewal) {
			logger.WithField("leaderIdentity", syncer.elm.Identity()).
				WithError(err).
				Info("Db syncer failed to renew leadership on pushing epoch data to db")
			return false, nil
		}

		logger.WithError(err).Error("Db syncer failed to save epoch data to db")
		return false, errors.WithMessage(err, "failed to save epoch data to db")
	}

	syncer.epochFrom += uint64(len(epochDataSlice))
	syncer.monitor.Update(syncer.epochFrom)

	for _, epdata := range epochDataSlice { // cache epoch pivot info for late use
		err := syncer.epochPivotWin.Push(epdata.GetPivotBlock())
		if err != nil {
			logger.WithField("pivotBlockEpoch", epdata.Number).WithError(err).Info(
				"Db syncer failed to push pivot block into epoch cache window",
			)

			syncer.epochPivotWin.Reset()
			break
		}
	}

	logger.WithFields(logrus.Fields{
		"newSyncFrom":   syncer.epochFrom,
		"finalSyncSize": len(epochDataSlice),
	}).Debug("Db syncer succeeded to sync epoch data range")

	return false, nil
}

func (syncer *DatabaseSyncer) doTicker(ctx context.Context, ticker *time.Timer) error {
	logrus.Debug("DB sync ticking")

	start := time.Now()
	complete, err := syncer.syncOnce(ctx)
	metrics.Registry.Sync.SyncOnceQps("cfx", "db", err).UpdateSince(start)

	if err != nil {
		ticker.Reset(syncer.syncIntervalNormal)
	} else if complete {
		ticker.Reset(syncer.syncIntervalNormal)
	} else {
		ticker.Reset(syncer.syncIntervalCatchUp)
	}

	return err
}

func (syncer *DatabaseSyncer) nextEpochTo(maxEpochTo uint64) (uint64, uint64) {
	epochTo := util.MinUint64(syncer.epochFrom+syncer.maxSyncEpochs-1, maxEpochTo)

	if epochTo < syncer.epochFrom {
		return epochTo, 0
	}

	syncSize := epochTo - syncer.epochFrom + 1
	return epochTo, syncSize
}

func (syncer *DatabaseSyncer) pivotSwitchRevert(ctx context.Context, revertTo uint64) error {
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
	err := syncer.db.PopnWithFinalizer(revertTo, func(tx *gorm.DB) error {
		return syncer.elm.Extend(ctx)
	})

	if err != nil {
		if errors.Is(err, store.ErrLeaderRenewal) {
			logger.WithField("leaderIdentity", syncer.elm.Identity()).
				WithError(err).
				Info("Db syncer failed to renew leadership on popping epoch data from db")
			return nil
		}

		logger.WithError(err).Error(
			"Db syncer failed to pop epoch data from db due to pivot switch",
		)
		return errors.WithMessage(err, "failed to pop epoch data from db")
	}

	// remove pivot data of reverted epoch from cache window
	syncer.epochPivotWin.Popn(revertTo)
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
	if pivotHash, ok := syncer.epochPivotWin.GetPivotHash(latestEpochNo); ok {
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

func (syncer *DatabaseSyncer) onLeadershipChanged(
	ctx context.Context, lm election.LeaderManager, gainedOrLost bool) {
	syncer.epochPivotWin.Reset()
	if !gainedOrLost && ctx.Err() != context.Canceled {
		logrus.WithField("leaderID", lm.Identity()).Warn("DB syncer lost HA leadership")
	}
}

func (syncer *DatabaseSyncer) OnStateChange(state monitor.HealthState, details ...string) {
	if len(syncer.cfxs) > 1 && state == monitor.Unhealthy {
		// Switch to the next cfx client if the sync progress is not healthy
		oldCfxIdx := syncer.cfxIdx.Load()
		newCfxIdx := (oldCfxIdx + 1) % uint32(len(syncer.cfxs))
		syncer.cfxIdx.Store(newCfxIdx)

		logrus.WithFields(logrus.Fields{
			"oldCfxIndex": oldCfxIdx,
			"newCfxIndex": newCfxIdx,
		}).Info("Switched to the next cfx client")
	}
}
