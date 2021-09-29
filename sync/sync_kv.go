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
	// The threshold gap between the latest epoch and some epoch after which the epochs are regarded as decayed.
	decayedEpochGapThreshold = 100000
	// The maximum continuous queries that get empty epoch data. It can be used to prevent endless empty query loop.
	maxContEmptyEpochQueries = 100
)

// KVCacheSyncer is used to sync blockchain data into kv cache against the latest state epoch.
type KVCacheSyncer struct {
	cfx   sdk.ClientOperator
	cache store.CacheStore

	syncIntervalNormal  time.Duration // interval to sync epoch data in normal status
	syncIntervalCatchUp time.Duration // interval to sync epoch data in catching up mode

	maxSyncEpochs          uint64       // maximum number of epochs to sync once
	syncWindow             *epochWindow // epoch sync window on which the sync polling depends
	continuousEmptyQueries uint32       // continuous empty epoch queries

	lastSubEpochNo uint64      // last received epoch number from subscription, which is used for pubsub validation
	subEpochCh     chan uint64 // receive the epoch from pub/sub to detect pivot chain switch or to update epoch sync window
	checkPointCh   chan bool   // checkpoint channel received to check epoch data
}

// NewKVCacheSyncer creates an instance of KVCacheSyncer to sync latest state epoch data.
func NewKVCacheSyncer(cfx sdk.ClientOperator, cache store.CacheStore) *KVCacheSyncer {
	syncer := &KVCacheSyncer{
		cfx:   cfx,
		cache: cache,

		syncIntervalNormal:  time.Millisecond * 500,
		syncIntervalCatchUp: time.Millisecond * 100,

		maxSyncEpochs: viper.GetUint64("sync.maxEpochs"),
		syncWindow:    newEpochWindow(decayedEpochGapThreshold),

		lastSubEpochNo: citypes.EpochNumberNil,
		subEpochCh:     make(chan uint64, viper.GetInt64("sync.sub.buffer")),
		checkPointCh:   make(chan bool, 2),
	}

	// Ensure epoch data not reverted
	if err := ensureStoreEpochDataOk(cfx, cache); err != nil {
		logrus.WithError(err).Fatal("Cache syncer failed to ensure latest state epoch data not reverted")
	}

	// Load last sync epoch information
	syncer.mustLoadLastSyncEpoch()

	return syncer
}

// Sync starts to sync epoch data from blockchain with specified cfx instance.
func (syncer *KVCacheSyncer) Sync(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	logger := logrus.WithField("syncWindow", syncer.syncWindow)
	logger.Info("Cache syncer starting to sync epoch data...")

	ticker := time.NewTicker(syncer.syncIntervalCatchUp)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Cache syncer shutdown ok")
			return
		case <-syncer.checkPointCh:
			if err := syncer.doCheckPoint(); err != nil {
				logger.WithError(err).Error("Cache syncer failed to do sync checkpoint")
			}
		case newEpoch := <-syncer.subEpochCh:
			syncer.handleNewEpoch(newEpoch)
		case <-ticker.C:
			if err := syncer.doTicker(ticker); err != nil {
				logger.WithError(err).Error("Cache syncer failed to sync epoch data")
			}
		}
	}
}

// Load last sync epoch from cache store to continue synchronization
func (syncer *KVCacheSyncer) mustLoadLastSyncEpoch() {
	_, maxEpoch, err := syncer.cache.GetGlobalEpochRange()
	if err == nil {
		syncer.syncWindow.reset(maxEpoch+1, maxEpoch)
	} else if !syncer.cache.IsRecordNotFound(err) {
		logrus.WithError(err).Fatal("Cache syncer failed to get global epoch range from cache store")
	}
}

// Do epoch data checking for synchronized epoch data in cache
func (syncer *KVCacheSyncer) doCheckPoint() error {
	logrus.Debug("Cache syncer doing checkpoint...")

	return ensureStoreEpochDataOk(syncer.cfx, syncer.cache)
}

// Revert the epoch data in cache store until to some epoch
func (syncer *KVCacheSyncer) pivotSwitchRevert(revertTo uint64) error {
	logrus.WithFields(logrus.Fields{
		"revertTo": revertTo, "syncWindow": syncer.syncWindow,
	}).Info("Reverting epoch data in cache due to pivot switch...")

	return syncer.cache.Popn(revertTo)
}

// Handle new epoch received to detect pivot switch or update epoch sync window
func (syncer *KVCacheSyncer) handleNewEpoch(newEpoch uint64) {
	logger := logrus.WithFields(logrus.Fields{
		"newEpoch": newEpoch, "beforeSyncWindow": *(syncer.syncWindow),
	})

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		logger.Debug("Cache syncer handling new epoch received...")
		defer logger.WithField("syncWindow", syncer.syncWindow).Debug("Cache syncer new received epoch handled")
	}

	// Peek if pivot switch will happen with the new epoch received
	if syncer.syncWindow.peekWillPivotSwitch(newEpoch) {
		logger.Debug("Cache syncer pivot switch detected")

		if err := syncer.pivotSwitchRevert(newEpoch); err == nil {
			syncer.syncWindow.reset(newEpoch, newEpoch) // reset sync window to start from the revert point again
		} else {
			logger.WithError(err).Error("Failed to remove epoch data in cache due to pivot switch")
		}

		return
	}

	// Peek if overflow will happen with the new epoch received
	if syncer.syncWindow.peekWillOverflow(newEpoch) {
		logger.Debug("Cache syncer sync window overflow detected")

		if err := syncer.cache.Flush(); err == nil { // flush all decayed old data in cache store
			syncer.syncWindow.reset(newEpoch, newEpoch)
		} else {
			logger.WithError(err).Error("Failed to flush decayed data in store due to sync window overflow")
		}

		return
	}

	// Update upper bound of the sync window to the new epoch received
	syncer.syncWindow.updateTo(newEpoch)
}

// Ticker to catch up or sync epoch data
func (syncer *KVCacheSyncer) doTicker(ticker *time.Ticker) error {
	logrus.Debug(">>> Cache syncer ticking...")

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

// Sync data once and return true if sync window is consumed to be empty, otherwise false.
func (syncer *KVCacheSyncer) syncOnce() (bool, error) {
	logger := logrus.WithField("syncWindow", syncer.syncWindow)

	if syncer.syncWindow.isEmpty() {
		logger.Debug("Cache syncer syncOnce skipped with epoch sync window empty")
		return true, nil
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

		// If epoch pivot switched, stop the querying right now since it's pointless to query epoch data
		// that will be reverted late.
		data, err := store.QueryEpochData(syncer.cfx, epochNo)
		if errors.Is(err, store.ErrEpochPivotSwitched) {
			eplogger.WithError(err).Info("Cache syncer failed to query epoch data due to pivot switch")
			break
		}

		if err != nil {
			eplogger.WithError(err).Error("Cache syncer failed to query epoch data")
			return false, errors.WithMessagef(err, "failed to query epoch data for epoch %v", epochNo)
		}

		eplogger.Debug("Cache syncer succeeded to query epoch data")
		epochDataSlice = append(epochDataSlice, &data)
	}

	if len(epochDataSlice) == 0 { // empty epoch data query
		syncer.continuousEmptyQueries++

		if syncer.continuousEmptyQueries >= maxContEmptyEpochQueries { // in case of endless empty query loop
			logger.WithFields(logrus.Fields{
				"continuousEmptyQueries": syncer.continuousEmptyQueries,
			}).Warn("Too many continuous empty epoch queries")
		}

		return false, nil
	}

	if err := syncer.cache.Pushn(epochDataSlice); err != nil {
		logger.WithError(err).Error("Cache syncer failed to write epoch data to cache store")
		return false, errors.WithMessage(err, "failed to write epoch data to cache store")
	}

	syncer.continuousEmptyQueries = 0 // reset continuous empty queries
	syncFrom, syncSize = syncer.syncWindow.shrinkFrom(uint32(len(epochDataSlice)))

	logger.WithFields(logrus.Fields{
		"syncFrom": syncFrom, "finalSyncSize": syncSize,
	}).Debug("Cache syncer succeeded to sync epoch data range")

	return syncer.syncWindow.isEmpty(), nil
}

// Validate new received epoch from pubsub to check if it's continous to the last received
// subscription epoch number or pivot switched.
func (syncer *KVCacheSyncer) validateNewReceivedEpoch(newEpoch uint64) error {
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
		err := errors.New("epoch not continuous")
		logger.WithError(err).Error("Cache syncer failed to validate new epoch from pubsub")

		return err
	}
}

// implement the EpochSubscriber interface.
func (syncer *KVCacheSyncer) onEpochReceived(epoch types.WebsocketEpochResponse) {
	epochNo := epoch.EpochNumber.ToInt().Uint64()

	logrus.WithField("epoch", epochNo).Debug("Cache syncer onEpochReceived new epoch received")

	if err := syncer.validateNewReceivedEpoch(epochNo); err != nil {
		// Failed to validate new epoch received from pubsub. This is serious because it might incur
		// data consistency problem.
		// TODO: Should we panic and exit for this?
		logrus.WithError(err).Error("!!! Cache syncer failed to validate new received epoch from pubsub")
	}

	syncer.subEpochCh <- epochNo
}

func (syncer *KVCacheSyncer) onEpochSubStart() {
	logrus.Debug("Cache syncer onEpochSubStart event received")

	atomic.StoreUint64(&(syncer.lastSubEpochNo), citypes.EpochNumberNil) // reset lastSubEpochNo
	syncer.checkPointCh <- true
}
