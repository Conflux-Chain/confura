package catchup

import (
	"context"
	"fmt"
	"sync"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Syncer accelerates epoch data catch-up using concurrently workers.
// Specifically, each worker will be dispatched as round-robin load
// balancing.
type Syncer struct {
	// goroutine workers to fetch epoch data concurrently
	workers []*worker
	// conflux sdk client delegated to get network status
	cfx sdk.ClientOperator
	// db store to persist epoch data
	db store.Store
	// specifying the epoch range to sync
	syncRange types.EpochRange
	// whether to automatically adjust target sync epoch number to the latest stable epoch,
	// which is maximum between the latest finalized and the checkpoint epoch number.
	adaptive bool
	// min num of db rows per batch persistence
	minBatchDbRows int
	// max num of epochs per batch persistence
	maxNumEpochs int
	// benchmark catch-up sync performance
	bmarker *benchmarker
}

// functional options for syncer
type SyncOption func(*Syncer)

func WithAdaptive(adaptive bool) SyncOption {
	return func(s *Syncer) {
		s.adaptive = adaptive
	}
}

func WithEpochFrom(epochFrom uint64) SyncOption {
	return func(s *Syncer) {
		s.syncRange.EpochFrom = epochFrom
	}
}

func WithEpochTo(epochTo uint64) SyncOption {
	return func(s *Syncer) {
		s.syncRange.EpochTo = epochTo
	}
}

func WithMinBatchDbRows(dbRows int) SyncOption {
	return func(s *Syncer) {
		s.minBatchDbRows = dbRows
	}
}

func WithMaxNumEpochs(maxNumEpochs int) SyncOption {
	return func(s *Syncer) {
		s.maxNumEpochs = maxNumEpochs
	}
}

func WithWorkers(workers []*worker) SyncOption {
	return func(s *Syncer) {
		s.workers = workers
	}
}

func WithBenchmark(benchmark bool) SyncOption {
	return func(s *Syncer) {
		if benchmark {
			s.bmarker = newBenchmarker()
		} else {
			s.bmarker = nil
		}
	}
}

func MustNewSyncer(cfx sdk.ClientOperator, db store.Store, opts ...SyncOption) *Syncer {
	var conf config
	viperutil.MustUnmarshalKey("sync.catchup", &conf)

	var workers []*worker
	for i, nodeUrl := range conf.CfxPool { // initialize workers
		name := fmt.Sprintf("CUWorker#%v", i)
		worker := mustNewWorker(name, nodeUrl, conf.WorkerChanSize)
		workers = append(workers, worker)
	}

	var newOpts []SyncOption
	newOpts = append(newOpts,
		WithMinBatchDbRows(conf.DbRowsThreshold),
		WithMaxNumEpochs(conf.epochsThreshold),
		WithWorkers(workers),
	)

	return newSyncer(cfx, db, append(newOpts, opts...)...)
}

func newSyncer(cfx sdk.ClientOperator, db store.Store, opts ...SyncOption) *Syncer {
	syncer := &Syncer{
		db: db, cfx: cfx, adaptive: true, minBatchDbRows: 1500,
	}

	for _, opt := range opts {
		opt(syncer)
	}

	return syncer
}

func (s *Syncer) Close() {
	for _, w := range s.workers {
		w.Close()
	}
}

func (s *Syncer) Sync(ctx context.Context) {
	s.logger().WithField("numWorkers", len(s.workers)).Debug(
		"Catch-up syncer starting to catch up latest epoch",
	)

	if len(s.workers) == 0 { // no workers configured?
		logrus.Debug("Catch-up syncer skipped due to not workers configured")
		return
	}

	if s.adaptive && !s.updateEpochTo(ctx) {
		logrus.Debug("Catch-up syncer skipped due to context canceled")
		return
	}

	if s.bmarker != nil {
		start := s.syncRange.EpochFrom

		s.bmarker.markStart()
		defer func() {
			s.bmarker.report(start, s.syncRange.EpochFrom)
		}()
	}

	for {
		start, end := s.syncRange.EpochFrom, s.syncRange.EpochTo
		if start > end || s.interrupted(ctx) {
			return
		}

		s.syncOnce(ctx, start, end)

		if s.adaptive && !s.updateEpochTo(ctx) {
			return
		}
	}
}

func (s *Syncer) syncOnce(ctx context.Context, start, end uint64) {
	var wg sync.WaitGroup

	wg.Add(1)
	go s.fetchResult(ctx, &wg, start, end)

	for i, w := range s.workers {
		wg.Add(1)

		wstart := start + uint64(i)
		stepN := uint64(len(s.workers))

		go w.Sync(ctx, &wg, wstart, end, stepN)
	}

	wg.Wait()
}

func (s *Syncer) fetchResult(ctx context.Context, wg *sync.WaitGroup, start, end uint64) {
	var epochData *store.EpochData
	var state persistState

	defer wg.Done()
	// do last db write anyway since there may be some epochs not
	// persisted yet.
	defer s.persist(&state)

	for eno := start; eno <= end; {
		for i := 0; i < len(s.workers) && eno <= end; i++ {
			w, startTime := s.workers[i], time.Now()

			select {
			case <-ctx.Done():
				return
			case epochData = <-w.Data():
				if s.bmarker != nil {
					s.bmarker.metricFetchPerEpochDuration(startTime)
				}

				// collect epoch data
				eno++
			}

			epochDbRows := state.update(epochData)

			logrus.WithFields(logrus.Fields{
				"workerName":  w.name,
				"epochNo":     epochData.Number,
				"epochDbRows": epochDbRows,
			}).Debug("Catch-up syncer collects new epoch data from worker")

			// bath insert into db if enough db rows or num of epochs collected
			if state.dbRows >= s.minBatchDbRows || state.numEpochs() >= s.maxNumEpochs {
				s.persist(&state)
			}
		}
	}
}

type persistState struct {
	dbRows int
	epochs []*store.EpochData
}

func (s *persistState) reset() {
	s.dbRows = 0
	s.epochs = []*store.EpochData{}
}

func (s *persistState) numEpochs() int {
	return len(s.epochs)
}

func (s *persistState) update(epochData *store.EpochData) int {
	epochDbRows := epochData.CalculateDbRows()

	s.epochs = append(s.epochs, epochData)
	s.dbRows += epochDbRows

	return epochDbRows
}

func (s *Syncer) persist(state *persistState) {
	numEpochs := state.numEpochs()
	if numEpochs == 0 {
		return
	}

	start := time.Now()
	defer func() {
		if s.bmarker != nil {
			s.bmarker.metricPersistDbRows(int64(state.dbRows))
			s.bmarker.metricPersistDuration(start)
		}

		state.reset()
	}()

	for {
		err := s.db.Pushn(state.epochs)
		if err == nil {
			break
		}

		logrus.WithError(err).Error("Catch-up syncer failed to persist epoch data")
		time.Sleep(time.Second)
	}

	s.syncRange.EpochFrom += uint64(numEpochs)
	s.logger().WithField("numEpochs", numEpochs).Debug("Catch-up syncer persisted epoch data")
}

func (s *Syncer) logger() *logrus.Entry {
	return logrus.WithFields(logrus.Fields{
		"epochFrom": s.syncRange.EpochFrom, "epochTo": s.syncRange.EpochTo,
	})
}

// updateEpochTo repeatedly try to update the target epoch number
func (s *Syncer) updateEpochTo(ctx context.Context) bool {
	for try := 1; ; try++ {
		if s.interrupted(ctx) {
			return false
		}

		err := s.doUpdateEpochTo()
		if err == nil {
			s.logger().Debug("Catch-up syncer updated epoch to number")
			return true
		}

		// TODO: refactor with time interval based logging
		logger := s.logger().WithError(err)

		logf := logger.Debug
		if try%50 == 0 {
			logf = logger.Error
		}

		logf("Catch-up worker failed to update epoch to number")
		time.Sleep(time.Second)
	}
}

// doUpdateEpochTo updates the target epoch number with the maximum epoch of the
// latest finalized or the latest checkpoint epoch for catch-up.
func (s *Syncer) doUpdateEpochTo() error {
	status, err := s.cfx.GetStatus()
	if err != nil {
		return errors.WithMessage(err, "failed to get network status")
	}

	s.syncRange.EpochTo = util.MaxUint64(
		uint64(status.LatestFinalized), uint64(status.LatestCheckpoint),
	)

	return nil
}

func (s *Syncer) interrupted(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}

	return false
}
