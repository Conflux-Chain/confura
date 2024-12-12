package catchup

import (
	"context"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util/metrics"
	cfxTypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/health"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// Task queue sizes
	pendingTaskQueueSize = 200
	recallTaskQueueSize  = 100

	// Task result queue sizes
	taskResultQueueSize = 200

	// Result channel size
	resultChanSize = 5000

	// Default task size and bounds
	defaultTaskSize = 50
	minTaskSize     = 1
	maxTaskSize     = 3000

	// Task size adjustment ratios
	incrementRatio = 0.2
	decrementRatio = 0.5

	// Maximum sample size for task size adjustment
	maxSampleSize = 200

	// Memory check interval
	memoryCheckInterval = 20 * time.Second

	// Force persistence interval
	forcePersistenceInterval = 30 * time.Second
)

var (
	// Memory health configuration
	memoryHealthCfg = health.CounterConfig{
		Threshold: 3, Remind: 3,
	}
)

// syncTask represents a range of epochs synchronization task
type syncTask struct {
	types.RangeUint64
}

// newSyncTask creates a new sync task with the given epoch range
func newSyncTask(start, end uint64) syncTask {
	return syncTask{RangeUint64: types.RangeUint64{From: start, To: end}}
}

// syncTaskResult holds the result of a completed syncTask.
type syncTaskResult struct {
	task      syncTask
	err       error
	epochData []*store.EpochData
}

func (res syncTaskResult) isSuccess() bool {
	return res.err == nil
}

func (res syncTaskResult) isInvalidFilterError() bool {
	return utils.IsRPCJSONError(res.err) &&
		strings.Contains(strings.ToLower(res.err.Error()), "filter error")
}

// coordinator orchestrates the synchronization process by:
//   - Managing pending tasks and recall tasks
//   - Adjusting task sizes dynamically
//   - Handling backpressure based on memory usage
//   - Collecting and ordering results for final persistence
type coordinator struct {
	// List of workers
	workers []*boostWorker

	// Full epoch range for synchronization
	fullEpochRange types.RangeUint64

	// Task queues
	pendingTaskQueue chan syncTask
	recallTaskQueue  chan syncTask

	// Task result queue
	taskResultQueue chan syncTaskResult

	// Synchronization state
	nextAssignEpoch uint64

	// Result pipeline
	nextWriteEpoch  uint64
	epochDataStore  map[uint64]*store.EpochData
	epochResultChan chan<- *store.EpochData

	// Backpressure control
	backpressureControl *atomic.Value
}

func newCoordinator(workers []*boostWorker, fullRange types.RangeUint64, resultChan chan<- *store.EpochData) *coordinator {
	backpressureControl := new(atomic.Value)
	backpressureControl.Store(make(chan struct{}))
	return &coordinator{
		workers:             workers,
		fullEpochRange:      fullRange,
		nextAssignEpoch:     fullRange.From,
		nextWriteEpoch:      fullRange.From,
		epochResultChan:     resultChan,
		epochDataStore:      make(map[uint64]*store.EpochData),
		pendingTaskQueue:    make(chan syncTask, pendingTaskQueueSize),
		recallTaskQueue:     make(chan syncTask, recallTaskQueueSize),
		taskResultQueue:     make(chan syncTaskResult, taskResultQueueSize),
		backpressureControl: backpressureControl,
	}
}

// backpressureChan returns the backpressure control channel
func (c *coordinator) backpressureChan() chan struct{} {
	return c.backpressureControl.Load().(chan struct{})
}

func (c *coordinator) run(ctx context.Context, wg *sync.WaitGroup) {
	// Start boost workers to process assigned tasks
	for _, w := range c.workers {
		wg.Add(1)
		go c.boostWorkerLoop(ctx, wg, w)
	}

	// Seeds the pending tasks queue with initial workload for workers.
	c.assignTasks(defaultTaskSize)

	// Start the result dispatch loop
	wg.Add(1)
	go c.dispatchLoop(ctx, wg)
}

// boostWorkerLoop continuously processes tasks assigned to the boostWorker.
func (c *coordinator) boostWorkerLoop(ctx context.Context, wg *sync.WaitGroup, w *boostWorker) {
	logrus.WithField("worker", w.name).Info("Boost worker started")
	defer logrus.WithField("worker", w.name).Info("Boost worker stopped")

	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.backpressureChan():
			time.Sleep(time.Second)
			continue
		default:
			select {
			case <-ctx.Done():
				return
			case <-c.backpressureChan():
				time.Sleep(time.Second)
				continue
			case task := <-c.pendingTaskQueue:
				epochData, err := w.fetchEpochData(task.From, task.To)
				if logrus.IsLevelEnabled(logrus.DebugLevel) {
					logrus.WithFields(logrus.Fields{
						"worker":       w.name,
						"task":         task,
						"numEpochData": len(epochData),
					}).WithError(err).Debug("Boost worker processed task")
				}
				c.taskResultQueue <- syncTaskResult{
					task:      task,
					epochData: epochData,
					err:       err,
				}
			}
		}
	}
}

// dispatchLoop collects results from workers, adjusts task sizes, and dispatches new tasks.
func (c *coordinator) dispatchLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	var resultHistory []syncTaskResult
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.backpressureChan():
			time.Sleep(time.Second)
			continue
		default:
			select {
			case <-ctx.Done():
				return
			case <-c.backpressureChan():
				time.Sleep(time.Second)
				continue
			case result := <-c.taskResultQueue:
				// Collect a batch of results
				taskResults := []syncTaskResult{result}
				for i := 0; i < len(c.taskResultQueue); i++ {
					taskResults = append(taskResults, <-c.taskResultQueue)
				}
				// Process the batch of results
				for _, r := range taskResults {
					if r.isSuccess() {
						resultHistory = append(resultHistory, r)
						// Collect epoch data
						c.collectEpochData(r.epochData)
					} else if r.isInvalidFilterError() && r.task.From < r.task.To {
						resultHistory = append(resultHistory, r)
						// Invalid filter: try splitting and reassigning
						c.recallTaskQueue <- r.task
					} else {
						// Other errors: retry the same task
						c.pendingTaskQueue <- r.task
					}
				}
				// Sort the task result history
				sort.Slice(resultHistory, func(i, j int) bool {
					r0, r1 := resultHistory[i], resultHistory[j]
					if r0.task.From == r1.task.From {
						return r0.task.To > r1.task.To
					}
					return r0.task.From < r1.task.From
				})
				// Retain the recent result history for estimation
				if len(resultHistory) > maxSampleSize {
					resultHistory = resultHistory[len(resultHistory)-maxSampleSize:]
				}
				// Estimate next task size and assign new tasks
				nextTaskSize := c.estimateTaskSize(resultHistory)
				c.assignTasks(nextTaskSize)
			}
		}
	}
}

// estimateTaskSize dynamically adjusts task size based on recent history.
func (c *coordinator) estimateTaskSize(results []syncTaskResult) uint64 {
	if len(results) == 0 {
		return defaultTaskSize
	}

	var totalEstSize, totalWeight float64
	for i, r := range results {
		weight := math.Pow(2, float64(1+i-len(results)))
		taskSize := float64(r.task.To - r.task.From + 1)
		var estSize float64
		if r.isSuccess() {
			estSize = taskSize * (1 + incrementRatio) * weight
		} else {
			estSize = taskSize * (1 - decrementRatio) * weight
		}
		totalEstSize += estSize
		totalWeight += weight
	}
	newTaskSize := uint64(totalEstSize / totalWeight)
	newTaskSize = min(max(minTaskSize, newTaskSize), maxTaskSize)
	return newTaskSize
}

// collectEpochData accumulates epoch data in memory until contiguous and flushes them in order.
func (c *coordinator) collectEpochData(result []*store.EpochData) {
	for _, data := range result {
		if c.nextWriteEpoch == data.Number {
			c.epochResultChan <- data
			c.nextWriteEpoch++
			continue
		}
		c.epochDataStore[data.Number] = data
	}

	// Flush any stored epochs that are now contiguous
	for {
		data, ok := c.epochDataStore[c.nextWriteEpoch]
		if !ok {
			break
		}
		c.epochResultChan <- data
		delete(c.epochDataStore, data.Number)
		c.nextWriteEpoch++
	}
}

// assignTasks schedules tasks for workers, handling recall tasks first if any.
func (c *coordinator) assignTasks(taskSize uint64) {
	for len(c.workers) > len(c.pendingTaskQueue) {
		// Handle recall tasks by splitting them into sub-tasks if possible
		if len(c.recallTaskQueue) > 0 {
			recallTask := <-c.recallTaskQueue
			midEpoch := (recallTask.From + recallTask.To) / 2
			c.pendingTaskQueue <- newSyncTask(recallTask.From, midEpoch)
			c.pendingTaskQueue <- newSyncTask(midEpoch+1, recallTask.To)
			continue
		}

		// The full epoch range has been assigned
		if c.nextAssignEpoch <= c.fullEpochRange.To {
			break
		}

		end := min(c.nextAssignEpoch+taskSize-1, c.fullEpochRange.To)
		c.pendingTaskQueue <- newSyncTask(c.nextAssignEpoch, end)
		c.nextAssignEpoch = end + 1
	}
}

// enableBackpressure toggles backpressure by closing or resetting the control channel.
func (c *coordinator) enableBackpressure(enabled bool) {
	if enabled {
		close(c.backpressureChan())
	} else {
		c.backpressureControl.Store(make(chan struct{}))
	}
}

type boostSyncer struct {
	*Syncer

	// List of boost workers
	workers []*boostWorker

	// Result channel
	resultChan chan *store.EpochData
}

func newBoostSyncer(s *Syncer) *boostSyncer {
	workers := make([]*boostWorker, len(s.workers))
	for i, w := range s.workers {
		workers[i] = &boostWorker{w}
	}
	return &boostSyncer{
		Syncer:     s,
		workers:    workers,
		resultChan: make(chan *store.EpochData, resultChanSize),
	}
}

func (s *boostSyncer) doSync(ctx context.Context, bmarker *benchmarker, start, end uint64) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)

	// Start coordinator
	fullEpochRange := types.RangeUint64{From: start, To: end}
	coord := newCoordinator(s.workers, fullEpochRange, s.resultChan)
	go coord.run(ctx, &wg)

	// Start memory monitor
	if s.memoryThreshold > 0 {
		go s.memoryMonitorLoop(ctx, coord)
	}

	// Start persisting results
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()

		err := s.fetchAndPersistResults(ctx, start, end, bmarker)
		if err != nil && !errors.Is(err, context.Canceled) {
			if errors.Is(err, store.ErrLeaderRenewal) {
				logrus.WithFields(logrus.Fields{
					"start":          start,
					"end":            end,
					"leaderIdentity": s.elm.Identity(),
				}).Info("Catch-up syncer failed to renew leadership on fetching result")
			} else {
				logrus.WithFields(logrus.Fields{
					"start": start,
					"end":   end,
				}).WithError(err).Error("Catch-up syncer failed to fetch result")
			}
		}
	}()

	wg.Wait()
}

// memoryMonitorLoop checks memory periodically and applies backpressure when memory is high.
func (s *boostSyncer) memoryMonitorLoop(ctx context.Context, c *coordinator) {
	ticker := time.NewTicker(memoryCheckInterval)
	defer ticker.Stop()

	// Counter to track memory health status
	var healthStatus health.Counter
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			logger := logrus.WithFields(logrus.Fields{
				"memory":    memStats.Alloc,
				"threshold": s.memoryThreshold,
			})
			logger.Debug("Memory usage checked for catch-up sync")

			// Backpressure control according to memory usage
			if memStats.Alloc < s.memoryThreshold {
				// Memory usage is below the threshold, try to lift backpressure.
				if recovered, _ := healthStatus.OnSuccess(memoryHealthCfg); recovered {
					logger.Warn("Catch-up sync memory usage has recovered below threshold")
					c.enableBackpressure(false)
				}
			} else {
				// Memory usage exceeds the threshold, check for health degradation.
				unhealthy, unrecovered, _ := healthStatus.OnFailure(memoryHealthCfg)
				if unhealthy {
					logger.Warn("Catch-up sync memory usage exceeded threshold")
					c.enableBackpressure(true)
				} else if unrecovered {
					logger.Warn("Catch-up sync memory usage remains above threshold")
				}
			}
		}
	}
}

// fetchAndPersistResults retrieves completed epoch data and persists them into the database.
func (s *boostSyncer) fetchAndPersistResults(ctx context.Context, start, end uint64, bmarker *benchmarker) error {
	var epochData *store.EpochData
	var state persistState

	timer := time.NewTimer(forcePersistenceInterval)
	defer timer.Stop()

	for eno := start; eno <= end; {
		startTime := time.Now()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case epochData = <-s.resultChan:
			// collect epoch data
			if epochData.Number != eno {
				return errors.Errorf("unexpected epoch collected, expected %v got %v", eno, epochData.Number)
			}
			if bmarker != nil {
				bmarker.metricFetchPerEpochDuration(startTime)
			}
			eno++
			s.monitor.Update(eno)
		}

		epochDbRows, storeDbRows := state.update(epochData)
		if logrus.IsLevelEnabled(logrus.DebugLevel) {
			logrus.WithFields(logrus.Fields{
				"epochNo":            epochData.Number,
				"epochDbRows":        epochDbRows,
				"storeDbRows":        storeDbRows,
				"state.insertDbRows": state.insertDbRows,
				"state.totalDbRows":  state.totalDbRows,
			}).Debug("Catch-up syncer collected new epoch data")
		}

		// Check if we need to persist now (due to timer expiration)
		var forcePersist bool
		select {
		case <-timer.C:
			forcePersist = true
		default:
		}

		// Batch insert into db if enough db rows collected, also use total db rows here to restrict memory usage.
		if forcePersist || state.totalDbRows >= s.maxDbRows || state.insertDbRows >= s.minBatchDbRows {
			if err := s.persist(ctx, &state, bmarker); err != nil {
				return err
			}

			state.reset()
			timer.Reset(forcePersistenceInterval)
		}
	}

	// Persist any remaining data
	return s.persist(ctx, &state, bmarker)
}

type boostWorker struct {
	*worker
}

// fetchEpochData fetches blocks and logs for a given epoch range to construct a minimal `EpochData`
// using `cfx_getLogs` for best peformance.
func (w *boostWorker) fetchEpochData(fromEpoch, toEpoch uint64) (res []*store.EpochData, err error) {
	if fromEpoch > toEpoch {
		return nil, errors.Errorf("invalid epoch range: from %v to %v", fromEpoch, toEpoch)
	}

	startTime := time.Now()
	defer func() {
		metrics.Registry.Sync.QueryEpochData("cfx", "catchup", "boost").UpdateSince(startTime)
		metrics.Registry.Sync.QueryEpochDataAvailability("cfx", "catchup", "boost").Mark(err == nil)
		metrics.Registry.Sync.QueryEpochRange("cfx").Update(int64(toEpoch - fromEpoch + 1))
	}()

	// Retrieve event logs within the specified epoch range
	logFilter := cfxTypes.LogFilter{
		FromEpoch: cfxTypes.NewEpochNumberUint64(fromEpoch),
		ToEpoch:   cfxTypes.NewEpochNumberUint64(toEpoch),
	}
	logs, err := w.cfx.GetLogs(logFilter)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get event logs")
	}

	var logCursor int
	for epochNum := fromEpoch; epochNum <= toEpoch; epochNum++ {
		// Initialize epoch data for the current epoch
		epochData := &store.EpochData{
			Number:   epochNum,
			Receipts: make(map[cfxTypes.Hash]*cfxTypes.TransactionReceipt),
		}

		// Cache to store blocks fetched by their hash to avoid repeated network calls
		blockCache := make(map[cfxTypes.Hash]*cfxTypes.Block)

		// Process logs that belong to the current epoch
		for ; logCursor < len(logs); logCursor++ {
			if logs[logCursor].EpochNumber.ToInt().Uint64() != epochNum {
				// Move to next epoch data construction if current log doesn't belong here
				break
			}

			// Retrieve or fetch the block associated with the current log
			blockHash := logs[logCursor].BlockHash
			if _, ok := blockCache[*blockHash]; !ok {
				var block *cfxTypes.Block
				block, err = w.cfx.GetBlockByHash(*blockHash)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to get block by hash %v", *blockHash)
				}

				// Add the block to the epoch data and cache
				epochData.Blocks = append(epochData.Blocks, block)
				blockCache[*blockHash] = block
			}

			// Retrieve or initialize the transaction receipt associated with the current log
			txnHash := logs[logCursor].TransactionHash
			txnReceipt, ok := epochData.Receipts[*txnHash]
			if !ok {
				txnReceipt = &cfxTypes.TransactionReceipt{
					EpochNumber:     (*hexutil.Uint64)(&epochNum),
					BlockHash:       *blockHash,
					TransactionHash: *txnHash,
				}

				epochData.Receipts[*txnHash] = txnReceipt
			}

			// Append the current log to the transaction receipt's logs
			txnReceipt.Logs = append(txnReceipt.Logs, logs[logCursor])
		}

		// Append the constructed epoch data to the result list
		res = append(res, epochData)
	}

	if logCursor != len(logs) {
		return nil, errors.Errorf("failed to process all logs: processed %v, total %v", logCursor, len(logs))
	}

	return res, nil
}
