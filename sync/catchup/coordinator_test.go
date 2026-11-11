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
