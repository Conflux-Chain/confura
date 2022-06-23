package sync

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/store/mysql"
	"github.com/conflux-chain/conflux-infura/sync/catchup"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/conflux-chain/conflux-infura/util/metrics"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// default capacity setting for pivot info window
	dbSyncEpochPivotWinCapacity = 400
)

// TODO: extract more sync config items
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
	db store.Store
	// epoch number to sync data from
	epochFrom uint64
	// maximum number of epochs to sync once
	maxSyncEpochs uint64
	// interval to sync data in normal status
	syncIntervalNormal time.Duration
	// interval to sync data in catching up mode
	syncIntervalCatchUp time.Duration
	// last received epoch number from pubsub for pivot chain switch detection
	lastSubEpochNo uint64
	// receive the pivot chain switched epoch event channel
	pivotSwitchEpochCh chan uint64
	// checkpoint channel received to check sync data
	checkPointCh chan bool
	// window to cache epoch pivot info
	epochPivotWin *epochPivotWindow
	// sync is ready only after fast catch-up is completed
	catchupCompleted uint32
}

// MustNewDatabaseSyncer creates an instance of DatabaseSyncer to sync blockchain data.
func MustNewDatabaseSyncer(cfx sdk.ClientOperator, db store.Store) *DatabaseSyncer {
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
		lastSubEpochNo:      citypes.EpochNumberNil,
		pivotSwitchEpochCh:  make(chan uint64, conf.Sub.Buffer),
		checkPointCh:        make(chan bool, 2),
		epochPivotWin:       newEpochPivotWindow(dbSyncEpochPivotWinCapacity),
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

	checkpoint := func() {
		if err := syncer.doCheckPoint(); err != nil {
			logrus.WithError(err).Error("Db syncer failed to do checkpoint")
			syncer.triggerCheckpoint() // re-trigger checkpoint
		}
	}

	breakLoop := false
	quit := func() {
		breakLoop = true
		logrus.Info("DB syncer shutdown ok")
	}

	syncer.fastCatchup(ctx)
	atomic.StoreUint32(&syncer.catchupCompleted, 1)

	ticker := time.NewTicker(syncer.syncIntervalCatchUp)
	defer ticker.Stop()

	for !breakLoop {
		select { // first class priority
		case <-ctx.Done():
			quit()
		case <-syncer.checkPointCh:
			checkpoint()
		default:
			select { // second class priority
			case <-ctx.Done():
				quit()
			case <-syncer.checkPointCh:
				checkpoint()
			case <-ticker.C:
				if err := syncer.doTicker(ticker); err != nil {
					logrus.WithError(err).
						WithField("epochFrom", syncer.epochFrom).
						Error("Db syncer failed to sync epoch data")
				}
			}
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
	if ms, ok := syncer.db.(*mysql.MysqlStore); ok && ms.V2 != nil {
		maxEpoch, ok, err := ms.V2.MaxEpoch()
		if err != nil {
			return false, errors.WithMessage(err, "failed to get max epoch from epoch to block mapping")
		}

		if ok {
			syncer.epochFrom = maxEpoch + 1
		}

		return ok, nil
	}

	_, maxEpoch, err := syncer.db.GetGlobalEpochRange()
	if err == nil {
		syncer.epochFrom = maxEpoch + 1
		return true, nil
	}

	if !syncer.db.IsRecordNotFound(err) {
		return false, errors.WithMessage(err, "failed to read sync epoch range from db")
	}

	return false, nil
}

// Sync data once and return true if catch up to the latest confirmed epoch, otherwise false.
func (syncer *DatabaseSyncer) syncOnce() (bool, error) {
	// Drain pivot switch reorg event channel to handle pivot chain reorg
	if err := syncer.drainPivotReorgEvents(); err != nil {
		return false, err
	}

	// Fetch latest confirmed epoch from blockchain
	epoch, err := syncer.cfx.GetEpochNumber(types.EpochLatestConfirmed)
	if err != nil {
		return false, errors.WithMessage(
			err, "failed to query the latest confirmed epoch number",
		)
	}

	maxEpochTo := epoch.ToInt().Uint64()
	if syncer.epochFrom > maxEpochTo { // cached up to the latest confirmed epoch?
		logrus.WithFields(logrus.Fields{
			"epochRange": citypes.RangeUint64{From: syncer.epochFrom, To: maxEpochTo},
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

func (syncer *DatabaseSyncer) doCheckPoint() error {
	logger := logrus.WithFields(logrus.Fields{
		"epochFrom":      syncer.epochFrom,
		"lastSubEpochNo": atomic.LoadUint64(&syncer.lastSubEpochNo),
	})

	logger.Info("Db syncer ensuring epoch data validity on pubsub checkpoint")

	if err := ensureStoreEpochDataOk(syncer.cfx, syncer.db); err != nil {
		logger.WithError(err).Info(
			"Db syncer failed to ensure epoch data validity on checkpoint",
		)

		return errors.WithMessage(err, "failed to ensure data validity")
	}

	if _, err := syncer.loadLastSyncEpoch(); err != nil {
		logger.WithError(err).Info(
			"Db syncer failed to reload last sync point on checkpoint",
		)

		return errors.WithMessage(err, "failed to reload last sync point")
	}

	syncer.epochPivotWin.popn(syncer.latestStoreEpoch())

	return nil
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

// implement the EpochSubscriber interface.
func (syncer *DatabaseSyncer) onEpochReceived(epoch types.WebsocketEpochResponse) {
	if atomic.LoadUint32(&syncer.catchupCompleted) != 1 { // not ready for sync yet
		return
	}

	epochNo := epoch.EpochNumber.ToInt().Uint64()

	logger := logrus.WithField("epoch", epochNo)
	logger.Debug("Db syncer onEpochReceived new epoch received")

	if err := syncer.detectPivotSwitchFromPubsub(&epoch); err != nil {
		logger.WithError(err).Error(
			"Db syncer failed to detect pivot chain switch from pubsub",
		)
	}
}

func (syncer *DatabaseSyncer) onEpochSubStart() {
	if atomic.LoadUint32(&syncer.catchupCompleted) != 1 { // not ready for sync yet
		return
	}

	logrus.Debug("DB sync onEpochSubStart event received")

	atomic.StoreUint64(&(syncer.lastSubEpochNo), citypes.EpochNumberNil) // reset lastSubEpochNo
	syncer.triggerCheckpoint()
}

func (syncer *DatabaseSyncer) nextEpochTo(maxEpochTo uint64) (uint64, uint64) {
	epochTo := util.MinUint64(syncer.epochFrom+syncer.maxSyncEpochs-1, maxEpochTo)

	if epochTo < syncer.epochFrom {
		return epochTo, 0
	}

	syncSize := epochTo - syncer.epochFrom + 1
	return epochTo, syncSize
}

func (syncer *DatabaseSyncer) drainPivotReorgEvents() error {
	for {
		select {
		case rEpoch := <-syncer.pivotSwitchEpochCh:
			if rEpoch >= syncer.epochFrom {
				break
			}

			logrus.WithFields(logrus.Fields{
				"revertToEpoch": rEpoch,
				"syncFromEpoch": syncer.epochFrom,
			}).Warn("Db syncer detected pivot chain reorg for the latest confirmed epoch from pubsub")

			// pivot switch reorg for the latest confirmed epoch
			if err := syncer.pivotSwitchRevert(rEpoch); err != nil {
				return errors.WithMessage(err, "failed to revert epoch(s) from pivot switch reorg channel")
			}
		default:
			return nil
		}
	}
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

func (syncer *DatabaseSyncer) triggerCheckpoint() {
	if len(syncer.checkPointCh) == 0 {
		syncer.checkPointCh <- true
	}
}

// Detect pivot chain switch by new received epoch from pubsub. Besides, it also validates if
// the new received epoch is continuous to the last received subscription epoch number.
func (syncer *DatabaseSyncer) detectPivotSwitchFromPubsub(epoch *types.WebsocketEpochResponse) error {
	newEpoch := epoch.EpochNumber.ToInt().Uint64()

	addrPtr := &(syncer.lastSubEpochNo)
	lastSubEpochNo := atomic.LoadUint64(addrPtr)

	var pivotHash types.Hash
	if len(epoch.EpochHashesOrdered) > 0 {
		pivotHash = epoch.EpochHashesOrdered[len(epoch.EpochHashesOrdered)-1]
	}

	logger := logrus.WithFields(logrus.Fields{
		"newEpoch": newEpoch, "lastSubEpochNo": lastSubEpochNo, "pivotHash": pivotHash,
	})

	switch {
	case lastSubEpochNo == citypes.EpochNumberNil: // initial state
		logger.Debug("Db syncer initially set last sub epoch number for pivot switch detection")

		atomic.StoreUint64(addrPtr, newEpoch)
	case lastSubEpochNo >= newEpoch: // pivot switch
		logger.Info("Db syncer detected pubsub new epoch pivot switched")

		atomic.StoreUint64(addrPtr, newEpoch)
		syncer.pivotSwitchEpochCh <- newEpoch
	case lastSubEpochNo+1 == newEpoch: // continuous
		logger.Debug("Db syncer validated continuous new epoch from pubsub")

		atomic.StoreUint64(addrPtr, newEpoch)
	default: // bad incontinuous epoch
		return errors.Errorf("bad incontinuous epoch, expect %v got %v", lastSubEpochNo+1, newEpoch)
	}

	return nil
}

func (syncer *DatabaseSyncer) getStoreLatestPivotHash() (types.Hash, error) {
	latestEpochNo := syncer.latestStoreEpoch()

	// load from in-memory cache first
	if pivotHash, ok := syncer.epochPivotWin.getPivotHash(latestEpochNo); ok {
		return pivotHash, nil
	}

	if ms, ok := syncer.db.(*mysql.MysqlStore); ok && ms.V2 != nil {
		pivotHash, _, err := ms.V2.PivotHash(latestEpochNo)
		return types.Hash(pivotHash), err
	}

	// load from db store if cache missed
	pivotBlock, err := syncer.db.GetBlockSummaryByEpoch(latestEpochNo)
	if err == nil {
		return pivotBlock.CfxBlockSummary.Hash, nil
	}

	if syncer.db.IsRecordNotFound(err) {
		return types.Hash(""), nil
	}

	return types.Hash(""), errors.WithMessagef(
		err, "failed to get block by epoch %v", latestEpochNo,
	)
}

func (syncer *DatabaseSyncer) latestStoreEpoch() uint64 {
	if syncer.epochFrom > 0 {
		return syncer.epochFrom - 1
	}

	return 0
}
