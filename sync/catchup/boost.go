package catchup

import (
	"container/heap"
	"context"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/go-conflux-util/health"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// Min priority queue capacity for shrink
	minPqShrinkCapacity = 100
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

// syncTaskItem is a heap item
type syncTaskItem struct {
	syncTask
	index int
}

// syncTaskPriorityQueue implements heap.Interface and is a min-heap.
type syncTaskPriorityQueue []*syncTaskItem

func (pq syncTaskPriorityQueue) Len() int { return len(pq) }

func (pq syncTaskPriorityQueue) Less(i, j int) bool {
	return pq[i].From < pq[j].From
}

func (pq syncTaskPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *syncTaskPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*syncTaskItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *syncTaskPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	if n == 0 {
		return nil
	}

	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]

	// Check if we need to shrink the underlying array to reduce memory usage
	if oldCap := cap(*pq); oldCap > minPqShrinkCapacity && len(*pq) < cap(*pq)/4 {
		newCap := 2 * len(*pq)
		newPq := make(syncTaskPriorityQueue, len(*pq), newCap)
		copy(newPq, *pq)
		*pq = newPq
	}
	return item
}

// syncTaskResult holds the result of a completed syncTask.
type syncTaskResult[T store.ChainData] struct {
	task   syncTask
	err    error
	result []T
}

// coordinator orchestrates the synchronization process by:
//   - Managing pending tasks and recall tasks
//   - Adjusting task sizes dynamically
//   - Handling backpressure based on memory usage
//   - Collecting and ordering results for final persistence
type coordinator[T store.ChainData] struct {
	// Configuration
	boostConfig

	// List of workers
	workers []*boostWorker[T]

	// Full epoch range for synchronization
	fullEpochRange types.RangeUint64

	// Task queues
	pendingTaskQueue chan syncTask

	// Recall task priority queue
	recallTaskPq syncTaskPriorityQueue

	// Task result queue
	taskResultQueue chan syncTaskResult[T]

	// Synchronization state
	nextAssignEpoch uint64

	// Result pipeline
	nextWriteEpoch  uint64
	epochDataStore  map[uint64]T
	epochResultChan chan<- T

	// Backpressure control
	backpressureControl *atomic.Value
}

func newCoordinator[T store.ChainData](
	cfg boostConfig, workers []*boostWorker[T], fullRange types.RangeUint64, resultChan chan<- T) *coordinator[T] {
	backpressureControl := new(atomic.Value)
	backpressureControl.Store(make(chan struct{}))
	return &coordinator[T]{
		boostConfig:         cfg,
		workers:             workers,
		fullEpochRange:      fullRange,
		nextAssignEpoch:     fullRange.From,
		nextWriteEpoch:      fullRange.From,
		epochResultChan:     resultChan,
		epochDataStore:      make(map[uint64]T),
		pendingTaskQueue:    make(chan syncTask, cfg.TaskQueueSize),
		taskResultQueue:     make(chan syncTaskResult[T], cfg.ResultQueueSize),
		backpressureControl: backpressureControl,
	}
}

// backpressureChan returns the backpressure control channel
func (c *coordinator[T]) backpressureChan() chan struct{} {
	return c.backpressureControl.Load().(chan struct{})
}

func (c *coordinator[T]) run(ctx context.Context, wg *sync.WaitGroup) {
	var innerWg sync.WaitGroup
	defer wg.Done()

	// Start boost workers to process assigned tasks
	for _, w := range c.workers {
		innerWg.Add(1)
		go c.boostWorkerLoop(ctx, &innerWg, w)
	}

	// Start the result dispatch loop
	innerWg.Add(1)
	go c.dispatchLoop(ctx, &innerWg)

	// Seeds the pending tasks queue with initial workload for workers.
	c.assignTasks(ctx, c.DefaultTaskSize)

	innerWg.Wait()
}

// boostWorkerLoop continuously processes tasks assigned to the boostWorker.
func (c *coordinator[T]) boostWorkerLoop(ctx context.Context, wg *sync.WaitGroup, w *boostWorker[T]) {
	logrus.WithField("worker", w.name).Info("Catch-up boost worker started")
	defer logrus.WithField("worker", w.name).Info("Catch-up boost worker stopped")

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
				data, err := w.queryChainData(task.From, task.To)
				if logrus.IsLevelEnabled(logrus.DebugLevel) {
					logrus.WithFields(logrus.Fields{
						"worker":          w.name,
						"task":            task,
						"numEpochData":    len(data),
						"numPendingTasks": len(c.pendingTaskQueue),
					}).WithError(err).Debug("Catch-up boost worker processed task")
				}
				c.taskResultQueue <- syncTaskResult[T]{
					task:   task,
					result: data,
					err:    err,
				}
			}
		}
	}
}

// dispatchLoop collects results from workers, adjusts task sizes, and dispatches new tasks.
func (c *coordinator[T]) dispatchLoop(ctx context.Context, wg *sync.WaitGroup) {
	logrus.Info("Catch-up boost coordinator dispatch loop started")
	defer logrus.Info("Catch-up boost coordinator dispatch loop stopped")

	var resultHistory []syncTaskResult[T]
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
			case result := <-c.taskResultQueue:
				// Collect a batch of results
				taskResults := []syncTaskResult[T]{result}
				for i := 0; i < len(c.taskResultQueue); i++ {
					taskResults = append(taskResults, <-c.taskResultQueue)
				}
				// Sort the task result
				sort.Slice(taskResults, func(i, j int) bool {
					r0, r1 := taskResults[i], taskResults[j]
					return r0.task.From < r1.task.From
				})
				// Process the batch of results
				for _, r := range taskResults {
					if r.err != nil {
						// Recall the task by splitting and re-assigning
						heap.Push(&c.recallTaskPq, &syncTaskItem{syncTask: r.task})
						resultHistory = append(resultHistory, r)
						continue
					}

					// Collect epoch data
					if err := c.collectResult(ctx, r.result); err != nil {
						return
					}
					resultHistory = append(resultHistory, r)
					r.result = nil // free memory
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
				if len(resultHistory) > c.MaxSampleSize {
					resultHistory = resultHistory[len(resultHistory)-c.MaxSampleSize:]
				}
				// Estimate next task size and assign new tasks
				nextTaskSize := c.estimateTaskSize(resultHistory)
				if err := c.assignTasks(ctx, nextTaskSize); err != nil {
					return
				}
			}
		}
	}
}

// estimateTaskSize dynamically adjusts task size based on recent history.
func (c *coordinator[T]) estimateTaskSize(results []syncTaskResult[T]) uint64 {
	if len(results) == 0 {
		return c.DefaultTaskSize
	}

	var totalEstSize, totalWeight float64
	for i, r := range results {
		weight := math.Pow(2, float64(1+i-len(results)))
		taskSize := float64(r.task.To - r.task.From + 1)
		var estSize float64
		if r.err == nil {
			estSize = taskSize * (1 + c.IncrementRatio) * weight
		} else {
			estSize = taskSize * (1 - c.DecrementRatio) * weight
		}
		totalEstSize += estSize
		totalWeight += weight
	}

	newTaskSize := uint64(math.Ceil(totalEstSize / totalWeight))
	newTaskSize = min(max(c.MinTaskSize, newTaskSize), c.MaxTaskSize)
	return newTaskSize
}

// collectResult accumulates results in memory until contiguous and flushes them in order.
func (c *coordinator[T]) collectResult(ctx context.Context, result []T) error {
	// Cache store epoch data
	for _, data := range result {
		c.epochDataStore[data.Number()] = data
	}

	// Flush any stored epochs that are now contiguous
	for {
		data, ok := c.epochDataStore[c.nextWriteEpoch]
		if !ok {
			break
		}

		if len(c.epochResultChan) >= c.WriteBufferSize {
			logrus.WithFields(logrus.Fields{
				"nextWriteEpoch": c.nextWriteEpoch,
				"numCacheEpochs": len(c.epochDataStore),
			}).Info("Catch-up boost sync write buffer is full")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.epochResultChan <- data:
			delete(c.epochDataStore, data.Number())
			c.nextWriteEpoch++
		}
	}
	return nil
}

// assignTasks schedules tasks for workers, handling recall tasks first if any.
func (c *coordinator[T]) assignTasks(ctx context.Context, taskSize uint64) error {
	for numPendingTasks := len(c.pendingTaskQueue); numPendingTasks < len(c.workers); numPendingTasks++ {
		// Handle recall tasks by splitting them into sub-tasks if possible
		if len(c.recallTaskPq) > 0 {
			recallTask := heap.Pop(&c.recallTaskPq).(*syncTaskItem).syncTask
			midEpoch := (recallTask.From + recallTask.To) / 2
			if err := c.addPendingTask(ctx, recallTask.From, midEpoch); err != nil {
				return err
			}
			if midEpoch+1 <= recallTask.To {
				numPendingTasks++
				if err := c.addPendingTask(ctx, midEpoch+1, recallTask.To); err != nil {
					return err
				}
			}
			continue
		}

		// The full epoch range has already been assigned
		if c.nextAssignEpoch > c.fullEpochRange.To {
			break
		}

		end := min(c.nextAssignEpoch+taskSize-1, c.fullEpochRange.To)
		if err := c.addPendingTask(ctx, c.nextAssignEpoch, end); err != nil {
			return err
		}
		c.nextAssignEpoch = end + 1
	}
	return nil
}

func (c *coordinator[T]) addPendingTask(ctx context.Context, start, end uint64) error {
	task := newSyncTask(start, end)
	if len(c.pendingTaskQueue) >= c.TaskQueueSize {
		logrus.WithFields(logrus.Fields{
			"toAddTask":       task,
			"nextAssignEpoch": c.nextAssignEpoch,
		}).Info("Catch-up boost pending task queue is full")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.pendingTaskQueue <- task:
		return nil
	}
}

// enableBackpressure toggles backpressure by closing or resetting the control channel.
func (c *coordinator[T]) enableBackpressure(enabled bool) {
	if enabled {
		close(c.backpressureChan())
	} else {
		c.backpressureControl.Store(make(chan struct{}))
	}
}

type boostSyncer[T store.ChainData] struct {
	*Syncer[T]

	// List of boost workers
	workers []*boostWorker[T]

	// Result channel
	resultChan chan T
}

func newBoostSyncer[T store.ChainData](s *Syncer[T]) *boostSyncer[T] {
	workers := make([]*boostWorker[T], len(s.workers))
	for i, w := range s.workers {
		workers[i] = &boostWorker[T]{w}
	}
	return &boostSyncer[T]{
		Syncer:     s,
		workers:    workers,
		resultChan: make(chan T, s.Boost.WriteBufferSize),
	}
}

func (s *boostSyncer[T]) doSync(ctx context.Context, bmarker *benchmarker, start, end uint64) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)

	// Start coordinator
	fullEpochRange := types.RangeUint64{From: start, To: end}
	coord := newCoordinator(s.Boost, s.workers, fullEpochRange, s.resultChan)

	wg.Add(1)
	go coord.run(ctx, &wg)

	// Start memory monitor
	if s.Boost.MemoryThreshold > 0 {
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
				}).Info("Catch-up boost syncer failed to renew leadership on fetching result")
			} else {
				logrus.WithFields(logrus.Fields{
					"start": start,
					"end":   end,
				}).WithError(err).Error("Catch-up boost syncer failed to fetch result")
			}
		}
	}()

	wg.Wait()
}

// memoryMonitorLoop checks memory periodically and applies backpressure when memory is high.
func (s *boostSyncer[T]) memoryMonitorLoop(ctx context.Context, c *coordinator[T]) {
	ticker := time.NewTicker(s.Boost.MemoryCheckInterval)
	defer ticker.Stop()

	// Counter to track memory health status
	healthStatus := health.NewCounter(memoryHealthCfg)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			logger := logrus.WithFields(logrus.Fields{
				"memory":    memStats.Alloc,
				"threshold": s.Boost.MemoryThreshold,
			})

			// Backpressure control according to memory usage
			if memStats.Alloc < s.Boost.MemoryThreshold {
				// Memory usage is below the threshold, try to lift backpressure.
				if recovered, _ := healthStatus.OnSuccess(); recovered {
					logger.Warn("Catch-up boost sync memory usage has recovered below threshold")
					c.enableBackpressure(false)
				}
			} else {
				// Memory usage exceeds the threshold, check for health degradation.
				unhealthy, unrecovered, _ := healthStatus.OnFailure()
				if unhealthy {
					logger.Warn("Catch-up boost sync memory usage exceeded threshold")
					c.enableBackpressure(true)
				} else if unrecovered {
					logger.Warn("Catch-up boost sync memory usage remains above threshold")
				}
			}
		}
	}
}

// fetchAndPersistResults retrieves completed epoch data and persists them into the database.
func (s *boostSyncer[T]) fetchAndPersistResults(ctx context.Context, start, end uint64, bmarker *benchmarker) error {
	forceInterval := s.Boost.ForcePersistenceInterval
	timer := time.NewTimer(forceInterval)
	defer timer.Stop()

	var state persistState[T]
	for eno := start; eno <= end; {
		forcePersist := false
		startTime := time.Now()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case epochData := <-s.resultChan:
			// collect epoch data
			if epochData.Number() != eno {
				return errors.Errorf("unexpected epoch collected, expected %v got %v", eno, epochData.Number())
			}
			if bmarker != nil {
				bmarker.metricFetchPerEpochDuration(startTime)
			}
			eno++
			s.monitor.Update(eno)

			epochDbRows, storeDbRows := state.update(s.filter, epochData)
			if logrus.IsLevelEnabled(logrus.DebugLevel) {
				logrus.WithFields(logrus.Fields{
					"resultBufLen":       len(s.resultChan),
					"epochNo":            epochData.Number(),
					"epochDbRows":        epochDbRows,
					"storeDbRows":        storeDbRows,
					"state.insertDbRows": state.insertDbRows,
					"state.totalDbRows":  state.totalDbRows,
				}).Debug("Catch-up boost syncer collected new epoch data")
			}
		case <-timer.C:
			// Force persist if timer expires
			forcePersist = true
		}

		// Batch insert into db if `forcePersist` is true or enough db rows collected.
		if forcePersist || state.insertDbRows >= s.DbRowsThreshold {
			if err := s.persist(ctx, &state, bmarker); err != nil {
				return err
			}

			state.reset()
			timer.Reset(forceInterval)
		}
	}

	// Persist any remaining data
	return s.persist(ctx, &state, bmarker)
}

type boostWorker[T store.ChainData] struct {
	*worker[T]
}

// queryChainData fetches blocks and logs for a given epoch range to construct a minimal chain data struct
// using `getLogs` rpc for best performance.
func (w *boostWorker[T]) queryChainData(fromEpoch, toEpoch uint64) (res []T, err error) {
	space := w.client.Space()
	startTime := time.Now()

	defer func() {
		metrics.Registry.Sync.BoostQueryEpochData(space).UpdateSince(startTime)
		metrics.Registry.Sync.BoostQueryEpochDataAvailability(space).Mark(err == nil)
		if err == nil {
			metrics.Registry.Sync.BoostQueryEpochRange().Update(int64(toEpoch - fromEpoch + 1))
		}
	}()

	res, err = w.client.BoostQueryChainData(context.Background(), fromEpoch, toEpoch)
	return
}
