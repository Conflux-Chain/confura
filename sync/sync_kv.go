package sync

import (
	"context"
	"sync"
	"sync/atomic"
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

const (
	// The threshold gap between the latest epoch and some epoch before
	// which the epochs are regarded as decayed.
	decayedEpochGapThreshold = 20000
)

// KVCacheSyncer is used to sync blockchain data into kv cache against
// the latest state epoch.
type KVCacheSyncer struct {
	// conflux sdk client
	cfx sdk.ClientOperator
	// redis store
	cache store.CacheStore
	// interval to sync epoch data in normal status
	syncIntervalNormal time.Duration
	// interval to sync epoch data in catching up mode
	syncIntervalCatchUp time.Duration
	// maximum number of epochs to sync once
	maxSyncEpochs uint64
	// epoch sync window on which the sync polling depends
	syncWindow *epochWindow
	// last received epoch number from subscription, which is used for
	// pubsub validation
	lastSubEpochNo uint64
	// receive the epoch from pub/sub to detect pivot chain switch or
	// to update epoch sync window
	subEpochCh chan uint64
	// checkpoint channel received to check epoch data
	checkPointCh chan bool
	// timer channel received to trigger sync task
	syncTimerCh <-chan time.Time
}

// MustNewKVCacheSyncer creates an instance of KVCacheSyncer to sync
// the latest state epoch data.
func MustNewKVCacheSyncer(cfx sdk.ClientOperator, cache store.CacheStore) *KVCacheSyncer {
	syncer := &KVCacheSyncer{
		cfx:                 cfx,
		cache:               cache,
		syncIntervalNormal:  time.Second,
		syncIntervalCatchUp: time.Millisecond,
		maxSyncEpochs:       viper.GetUint64("sync.maxEpochs"),
		syncWindow:          newEpochWindow(decayedEpochGapThreshold),
		lastSubEpochNo:      citypes.EpochNumberNil,
		subEpochCh:          make(chan uint64, viper.GetInt64("sync.sub.buffer")),
		checkPointCh:        make(chan bool, 2),
	}

	// Ensure epoch data validity in redis
	if err := ensureStoreEpochDataOk(cfx, cache); err != nil {
		logrus.WithError(err).Fatal(
			"KV syncer failed to ensure epoch data validity in redis",
		)
	}

	// Load last sync epoch information
	if _, err := syncer.loadLastSyncEpoch(); err != nil {
		logrus.WithError(err).Fatal(
			"Failed to load last sync epoch range from cache",
		)
	}

	return syncer
}

// Sync starts to sync epoch data from blockchain to cache.
func (syncer *KVCacheSyncer) Sync(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	logger := logrus.WithField("syncWindow", syncer.syncWindow)
	logger.Info("Cache syncer starting to sync epoch data...")

	ticker := time.NewTicker(syncer.syncIntervalNormal)
	defer ticker.Stop()

	checkpoint := func() {
		if err := syncer.doCheckPoint(); err != nil {
			logger.WithError(err).Error("Cache syncer failed to do checkpoint")

			syncer.triggerCheckpoint() // re-trigger checkpoint
		}
	}

	breakLoop := false
	quit := func() {
		breakLoop = true
		logrus.Info("Cache syncer shutdown ok")
	}

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
			case newEpoch := <-syncer.subEpochCh:
				if err := syncer.handleNewEpoch(newEpoch, ticker); err != nil {
					syncer.syncTimerCh = nil
					logger.WithField("newEpoch", newEpoch).WithError(err).Error(
						"Cache syncer failed to handle new received epoch",
					)
				}
			case <-syncer.syncTimerCh:
				if err := syncer.syncOnce(); err != nil {
					logger.WithError(err).Error("Cache syncer failed to sync epoch data")
				}

				if syncer.syncWindow.isEmpty() {
					syncer.syncTimerCh = nil
				}
			}
		}
	}
}

// Do epoch data checking for synchronized epoch data in cache
func (syncer *KVCacheSyncer) doCheckPoint() error {
	logger := logrus.WithFields(logrus.Fields{
		"syncWindow":     syncer.syncWindow,
		"lastSubEpochNo": atomic.LoadUint64(&syncer.lastSubEpochNo),
	})

	logger.Info("Cache syncer ensuring epoch data validity on pubsub checkpoint")

	if err := ensureStoreEpochDataOk(syncer.cfx, syncer.cache); err != nil {
		logger.WithError(err).Info(
			"Cache syncer failed to ensure epoch data validity on checkpoint",
		)

		return errors.WithMessage(err, "failed to ensure data validity")
	}

	if _, err := syncer.loadLastSyncEpoch(); err != nil {
		logger.WithError(err).Info(
			"Cache syncer failed to reload last sync point on checkpoint",
		)

		return errors.WithMessage(err, "failed to reload last sync point")
	}

	return nil
}

// Revert the epoch data in cache store until to some epoch
func (syncer *KVCacheSyncer) pivotSwitchRevert(revertTo uint64) error {
	if revertTo == 0 {
		return errors.New("genesis epoch must not be reverted")
	}

	logger := logrus.WithFields(logrus.Fields{
		"revertTo":   revertTo,
		"syncWindow": syncer.syncWindow,
	})

	logger.Info("Cache syncer reverting epoch data due to pivot chain switch")

	// remove epoch data from database due to pivot switch
	if err := syncer.cache.Popn(revertTo); err != nil {
		logger.WithError(err).Info(
			"Cache syncer failed to pop epoch data from redis due to pivot switch",
		)

		return errors.WithMessage(err, "failed to pop epoch data from redis")
	}

	// reset sync window to start from the revert point again
	syncer.syncWindow.reset(revertTo, revertTo)

	return nil
}

// Handle new epoch received to detect pivot switch or update epoch sync window
func (syncer *KVCacheSyncer) handleNewEpoch(newEpoch uint64, syncTicker *time.Ticker) error {
	logger := logrus.WithFields(logrus.Fields{
		"newEpoch":         newEpoch,
		"beforeSyncWindow": *(syncer.syncWindow),
	})

	if syncer.syncWindow.peekWillOverflow(newEpoch) { // peek overflow
		logger.Info("Cache syncer sync window overflow detected")

		if err := syncer.cache.Flush(); err != nil {
			return errors.WithMessage(
				err, "failed to flush decayed data in cache due to window overflow",
			)
		}

		syncer.syncWindow.reset(newEpoch, newEpoch)

	} else if syncer.syncWindow.peekWillPivotSwitch(newEpoch) { // peek pivot switch
		logger.Info("Cache syncer pivot switch detected")

		if err := syncer.pivotSwitchRevert(newEpoch); err != nil {
			return errors.WithMessage(
				err, "failed to remove epoch data in cache due to pivot switch",
			)
		}
	} else { // expand the sync window to the new epoch received
		syncer.syncWindow.updateTo(newEpoch)
	}

	// dynamically adjust the sync frequency
	syncWinSize := uint64(syncer.syncWindow.size())
	switch {
	case syncWinSize == 0:
		syncer.syncTimerCh = nil
	case syncWinSize > syncer.maxSyncEpochs:
		syncTicker.Reset(syncer.syncIntervalCatchUp)
		syncer.syncTimerCh = syncTicker.C
	default:
		syncTicker.Reset(syncer.syncIntervalNormal)
		syncer.syncTimerCh = syncTicker.C
	}

	return nil
}

// Sync data once for batch.
func (syncer *KVCacheSyncer) syncOnce() error {
	logger := logrus.WithField("syncWindow", syncer.syncWindow)

	if syncer.syncWindow.isEmpty() {
		logger.Debug("Cache syncer syncOnce skipped with epoch sync window empty")
		return nil
	}

	updater := metrics.NewTimerUpdaterByName("infura/duration/cache/sync/once")
	defer updater.Update()

	syncFrom, syncSize := syncer.syncWindow.peekShrinkFrom(uint32(syncer.maxSyncEpochs))

	syncSizeGauge := gometrics.GetOrRegisterGauge("infura/cache/sync/size/stated", nil)
	syncSizeGauge.Update(int64(syncSize))

	logger = logger.WithFields(logrus.Fields{"syncFrom": syncFrom, "syncSize": syncSize})
	logger.Debug("Cache syncer starting to sync epoch(s)...")

	epochDataSlice := make([]*store.EpochData, 0, syncSize)
	for i := uint32(0); i < syncSize; i++ {
		epochNo := syncFrom + uint64(i)
		eplogger := logger.WithField("epoch", epochNo)

		data, err := store.QueryEpochData(syncer.cfx, epochNo)

		// If epoch pivot switched, stop the querying right now since it's pointless to query epoch data
		// that will be reverted late.
		if errors.Is(err, store.ErrEpochPivotSwitched) {
			eplogger.WithError(err).Info("Cache syncer failed to query epoch data due to pivot switch")
			break
		}

		if err != nil {
			return errors.WithMessagef(err, "failed to query epoch data for epoch %v", epochNo)
		}

		if i == 0 { // the first epoch must be continuous to the latest epoch in cache store
			latestPivotHash, err := syncer.getStoreLatestPivotHash()
			if err != nil {
				eplogger.WithError(err).Error(
					"Cache syncer failed to get latest pivot hash from redis for parent hash check",
				)
				return errors.WithMessage(err, "failed to get latest pivot hash")
			}

			if len(latestPivotHash) > 0 && data.GetPivotBlock().ParentHash != latestPivotHash {
				eplogger.WithFields(logrus.Fields{
					"latestStoreEpoch": syncFrom - 1,
					"latestPivotHash":  latestPivotHash,
				}).Info("Cache syncer popping latest epoch from redis store due to parent hash mismatched")

				if err := syncer.pivotSwitchRevert(syncFrom - 1); err != nil {
					eplogger.WithError(err).Error(
						"Cache syncer failed to pop latest epoch from redis store due to parent hash mismatched",
					)

					return errors.WithMessage(
						err, "failed to pop latest epoch from redis store due to parent hash mismatched",
					)
				}

				return nil
			}
		} else { // otherwise non-first epoch must also be continuous to previous one
			continuous, desc := data.IsContinuousTo(epochDataSlice[i-1])
			if !continuous {
				// truncate the batch synced epoch data until the previous epoch
				epochDataSlice = epochDataSlice[:i-1]

				eplogger.WithField("i", i).Infof(
					"Cache syncer truncated batch synced data due to epoch not continuous for %v", desc,
				)
				break
			}
		}

		epochDataSlice = append(epochDataSlice, &data)

		eplogger.Debug("Cache syncer succeeded to query epoch data")
	}

	if len(epochDataSlice) == 0 { // empty epoch data query
		logger.Debug("Cache syncer skipped due to empty sync range")
		return nil
	}

	if err := syncer.cache.Pushn(epochDataSlice); err != nil {
		logger.WithError(err).Error("Cache syncer failed to push epoch data to redis store")
		return errors.WithMessage(err, "failed to push epoch data to redis store")
	}

	syncFrom, syncSize = syncer.syncWindow.shrinkFrom(uint32(len(epochDataSlice)))

	logger.WithFields(logrus.Fields{
		"newSyncFrom": syncFrom, "finalSyncSize": syncSize,
	}).Debug("Cache syncer succeeded to sync epoch data range")

	return nil
}

// Validate new received epoch from pubsub to check if it's continous to the last received
// subscription epoch number or pivot switched.
func (syncer *KVCacheSyncer) validateNewReceivedEpoch(epoch *types.WebsocketEpochResponse) error {
	newEpoch := epoch.EpochNumber.ToInt().Uint64()

	addrPtr := &(syncer.lastSubEpochNo)
	lastSubEpochNo := atomic.LoadUint64(addrPtr)

	logger := logrus.WithFields(logrus.Fields{
		"newEpoch": newEpoch, "lastSubEpochNo": lastSubEpochNo,
	})

	switch {
	case lastSubEpochNo == citypes.EpochNumberNil: // initial state
		logger.Debug("Cache syncer initially set last sub epoch number for validation")

		atomic.StoreUint64(addrPtr, newEpoch)
		return nil
	case lastSubEpochNo >= newEpoch: // pivot switch
		logger.Info("Cache syncer validated pubsub new epoch pivot switched")

		atomic.StoreUint64(addrPtr, newEpoch)
		return nil
	case lastSubEpochNo+1 == newEpoch: // continuous
		logger.Debug("Cache syncer validated pubsub new epoch continuous")

		atomic.StoreUint64(addrPtr, newEpoch)
		return nil
	default: // bad incontinuous epoch
		return errors.Errorf("bad incontinuous epoch, expect %v got %v", lastSubEpochNo+1, newEpoch)
	}
}

func (syncer *KVCacheSyncer) getStoreLatestPivotHash() (types.Hash, error) {
	if !syncer.syncWindow.isSet() {
		return types.Hash(""), nil
	}

	latestEpochNo := syncer.syncWindow.epochFrom - 1

	// load from cache store
	pivotBlock, err := syncer.cache.GetBlockSummaryByEpoch(latestEpochNo)
	if err == nil {
		return pivotBlock.Hash, nil
	}

	if syncer.cache.IsRecordNotFound(err) {
		return types.Hash(""), nil
	}

	return types.Hash(""), errors.WithMessagef(
		err, "failed to get block by epoch %v", latestEpochNo,
	)
}

func (syncer *KVCacheSyncer) triggerCheckpoint() {
	if len(syncer.checkPointCh) == 0 {
		syncer.checkPointCh <- true
	}
}

// Load last sync epoch from cache store to continue synchronization.
func (syncer *KVCacheSyncer) loadLastSyncEpoch() (loaded bool, err error) {
	_, maxEpoch, err := syncer.cache.GetGlobalEpochRange()
	if err == nil {
		syncer.syncWindow.reset(maxEpoch+1, maxEpoch)
		return true, nil
	}

	if !syncer.cache.IsRecordNotFound(err) {
		return false, errors.WithMessage(
			err, "failed to get global epoch range from cache",
		)
	}

	return false, nil
}

// implement the EpochSubscriber interface.

func (syncer *KVCacheSyncer) onEpochReceived(epoch types.WebsocketEpochResponse) {
	epochNo := epoch.EpochNumber.ToInt().Uint64()

	logger := logrus.WithField("epoch", epochNo)
	logger.Debug("Cache syncer onEpochReceived new epoch received")

	if err := syncer.validateNewReceivedEpoch(&epoch); err != nil {
		logger.WithError(err).Error(
			"Cache syncer failed to validate new received epoch from pubsub",
		)

		// reset lastSubEpochNo
		atomic.StoreUint64(&(syncer.lastSubEpochNo), citypes.EpochNumberNil)
		return
	}

	syncer.subEpochCh <- epochNo
}

func (syncer *KVCacheSyncer) onEpochSubStart() {
	logrus.Debug("Cache syncer onEpochSubStart event received")

	// reset lastSubEpochNo
	atomic.StoreUint64(&(syncer.lastSubEpochNo), citypes.EpochNumberNil)
	syncer.triggerCheckpoint()
}
