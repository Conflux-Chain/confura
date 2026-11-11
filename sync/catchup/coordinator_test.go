package catchup

import (
	"context"
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
