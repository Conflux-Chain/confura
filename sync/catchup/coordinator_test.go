package catchup

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/stretchr/testify/require"
)

func coordinatorConfig() boostConfig {
	return boostConfig{TaskQueueSize: 128, ResultQueueSize: 128, WriteBufferSize: 128, DefaultTaskSize: 1, MinTaskSize: 1, MaxTaskSize: 2, MaxSampleSize: 4}
}
func TestCoordinatorStartupAssignments(t *testing.T) {
	for range 20 {
		cli := &testClient{query: func(_ context.Context, a, b uint64) (r []*store.EthData, err error) {
			for n := a; n <= b; n++ {
				r = append(r, testData(n))
			}
			return
		}}
		var workers []*boostWorker[*store.EthData]
		for range 4 {
			workers = append(workers, &boostWorker[*store.EthData]{mustNewWorker("test", cli, 1)})
		}
		out := make(chan *store.EthData, 128)
		c := newCoordinator(coordinatorConfig(), workers, types.RangeUint64{From: 1, To: 64}, out)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		var wg sync.WaitGroup
		wg.Add(1)
		go c.run(ctx, &wg)
		var got []uint64
	collect:
		for len(got) < 64 {
			select {
			case d := <-out:
				got = append(got, d.Number())
			case <-ctx.Done():
				break collect
			}
		}
		cancel()
		wg.Wait()
		require.Len(t, got, 64)
		for i, n := range got {
			require.EqualValues(t, i+1, n)
		}
	}
}

func TestBoostWorkerCanceledWithNoResultConsumer(t *testing.T) {
	for _, size := range []int{0, 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			cli := &testClient{query: func(context.Context, uint64, uint64) ([]*store.EthData, error) {
				close(started)
				<-release
				return []*store.EthData{testData(1)}, nil
			}}
			cfg := coordinatorConfig()
			cfg.ResultQueueSize = size
			w := &boostWorker[*store.EthData]{mustNewWorker("test", cli, 1)}
			c := newCoordinator(cfg, []*boostWorker[*store.EthData]{w}, types.RangeUint64{From: 1, To: 1}, nil)
			if size > 0 {
				c.taskResultQueue <- syncTaskResult[*store.EthData]{}
			}
			c.pendingTaskQueue <- newSyncTask(1, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var wg sync.WaitGroup
			wg.Add(1)
			done := make(chan struct{})
			go func() { c.boostWorkerLoop(ctx, &wg, w); close(done) }()
			<-started
			cancel()
			close(release)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("worker blocked sending to abandoned result queue")
			}
			wg.Wait()
		})
	}
}

func TestTaskResultCoverage(t *testing.T) {
	task := newSyncTask(1, 3)
	for _, bad := range [][]*store.EthData{nil, {testData(1), testData(3)}, {testData(1), testData(1), testData(3)}, {testData(1), nil, testData(3)}, {testData(1), testData(2), testData(4)}} {
		require.Error(t, validateTaskResult(task, bad))
	}
	require.NoError(t, validateTaskResult(task, []*store.EthData{testData(3), testData(1), testData(2)}))
	out := make(chan *store.EthData, 3)
	c := newCoordinator(coordinatorConfig(), nil, types.RangeUint64{From: 1, To: 3}, out)
	require.NoError(t, c.collectResult(context.Background(), []*store.EthData{testData(3), testData(1), testData(2)}))
	for n := uint64(1); n <= 3; n++ {
		require.Equal(t, n, (<-out).Number())
	}
	require.NoError(t, c.collectResult(context.Background(), []*store.EthData{testData(1)}))
	require.Empty(t, c.epochDataStore)
}

func TestCoordinatorProgressUnderBackpressure(t *testing.T) {
	for _, queueSize := range []int{0, 1} {
		t.Run(fmt.Sprint(queueSize), func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			cli := new(testClient)
			// Fail the first range once; recalled singleton ranges are then valid.
			var once sync.Once
			cli.query = func(_ context.Context, a, b uint64) (r []*store.EthData, err error) {
				failed := false
				if a == 1 {
					once.Do(func() { close(started); <-release; failed = true })
				}
				if failed {
					return nil, fmt.Errorf("retry earliest task")
				}
				for n := a; n <= b; n++ {
					r = append(r, testData(n))
				}
				return
			}
			cfg := coordinatorConfig()
			cfg.TaskQueueSize = queueSize
			cfg.ResultQueueSize = queueSize
			cfg.DefaultTaskSize = 2
			var workers []*boostWorker[*store.EthData]
			for range 2 {
				workers = append(workers, &boostWorker[*store.EthData]{mustNewWorker("test", cli, 1)})
			}
			out := make(chan *store.EthData)
			c := newCoordinator(cfg, workers, types.RangeUint64{From: 1, To: 12}, out)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			wg.Add(1)
			go c.run(ctx, &wg)
			<-started
			for range 3 {
				c.enableBackpressure(true)
				c.enableBackpressure(false)
			}
			c.enableBackpressure(true)
			close(release)
			var got []uint64
		collect:
			for len(got) < 12 {
				select {
				case d := <-out:
					got = append(got, d.Number())
				case <-ctx.Done():
					break collect
				}
			}
			cancel()
			wg.Wait()
			require.Len(t, got, 12)
			for i, n := range got {
				require.EqualValues(t, i+1, n)
			}
			require.Empty(t, c.epochDataStore)
		})
	}
}

func TestMemoryMonitorAllowsProgress(t *testing.T) {
	cfg := coordinatorConfig()
	cfg.MemoryThreshold = 1 // The real monitor must keep pressure enabled throughout the run.
	cfg.MemoryCheckInterval = time.Millisecond
	cli := &testClient{query: func(_ context.Context, a, b uint64) (r []*store.EthData, err error) {
		for n := a; n <= b; n++ {
			r = append(r, testData(n))
		}
		return
	}}
	w := &boostWorker[*store.EthData]{mustNewWorker("test", cli, 1)}
	out := make(chan *store.EthData)
	c := newCoordinator(cfg, []*boostWorker[*store.EthData]{w}, types.RangeUint64{From: 1, To: 8}, out)
	s := &boostSyncer[*store.EthData]{Syncer: &Syncer[*store.EthData]{config: config{Boost: cfg}}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.memoryMonitorLoop(ctx, c) }()
	require.Eventually(t, c.isBackpressured, time.Second, time.Millisecond)
	wg.Add(1)
	go c.run(ctx, &wg)
	var got []uint64
	for len(got) < 8 {
		select {
		case d := <-out:
			got = append(got, d.Number())
		case <-ctx.Done():
			cancel()
			wg.Wait()
			t.Fatal("monitor stalled the earliest outstanding task")
		}
	}
	cancel()
	wg.Wait()
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8}, got)
	require.Empty(t, c.epochDataStore)
}

func TestCoordinatorKeepsDefaultPrefetch(t *testing.T) {
	c := newCoordinator(coordinatorConfig(), make([]*boostWorker[*store.EthData], 2), types.RangeUint64{From: 1, To: 20}, nil)
	require.NoError(t, c.assignTasks(context.Background(), 1))
	for range 2 {
		<-c.pendingTaskQueue // Both workers took their initial task.
	}
	// A result can trigger replenishment while another worker is still busy.
	require.NoError(t, c.assignTasks(context.Background(), 1))
	require.EqualValues(t, 5, c.nextAssignEpoch)
	require.Len(t, c.pendingTaskQueue, 2)
}

func TestCoordinatorKeepsHeightOrderedEstimation(t *testing.T) {
	cfg := coordinatorConfig()
	cfg.MaxTaskSize = 1000
	cfg.IncrementRatio, cfg.DecrementRatio = 0.2, 0.5
	c := newCoordinator(cfg, make([]*boostWorker[*store.EthData], 3), types.RangeUint64{From: 1, To: 1000}, nil)
	c.nextAssignEpoch = 201
	var data []*store.EthData
	for n := uint64(101); n <= 200; n++ {
		data = append(data, testData(n))
	}
	// Completion order is reversed. The original batch/history ordering must
	// still prioritize retries and give the later height the larger weight.
	c.taskResultQueue <- syncTaskResult[*store.EthData]{task: newSyncTask(101, 200), result: data}
	c.taskResultQueue <- syncTaskResult[*store.EthData]{task: newSyncTask(1, 100), err: fmt.Errorf("retry")}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go c.dispatchLoop(ctx, &wg)
	var tasks []syncTask
	for range 3 {
		select {
		case task := <-c.pendingTaskQueue:
			tasks = append(tasks, task)
		case <-ctx.Done():
		}
	}
	cancel()
	wg.Wait()
	require.Equal(t, []syncTask{newSyncTask(1, 50), newSyncTask(51, 100), newSyncTask(201, 297)}, tasks)
}

func TestCoordinatorBoundsLookaheadWithMemoryControl(t *testing.T) {
	cfg := coordinatorConfig()
	cfg.MemoryThreshold = 1
	c := newCoordinator(cfg, make([]*boostWorker[*store.EthData], 2), types.RangeUint64{From: 1, To: 20}, nil)
	c.nextAssignEpoch = 5 // The initial four heights are still outstanding.
	require.NoError(t, c.assignTasks(context.Background(), 2))
	require.Empty(t, c.pendingTaskQueue)
	c.nextWriteEpoch = 3 // Completing the earliest task permits more work.
	require.NoError(t, c.assignTasks(context.Background(), 2))
	require.Len(t, c.pendingTaskQueue, 1)
	require.Equal(t, newSyncTask(5, 6), <-c.pendingTaskQueue)
}
