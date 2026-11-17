package catchup

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/sync/election"
	"github.com/stretchr/testify/require"
)

func TestSyncOnceRejectsEmptyWorkers(t *testing.T) {
	s := &Syncer[*store.EthData]{}
	require.ErrorContains(t, s.SyncOnce(context.Background(), 1, 2), "worker")
	require.ErrorContains(t, s.SyncOnce(context.Background(), 2, 1), "range")
}

func TestSyncOnceReportsWorkerErrorAndCancellation(t *testing.T) {
	for _, boost := range []bool{false, true} {
		for _, cancelQuery := range []bool{false, true} {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			failure := errors.New("node unavailable")
			w := mustNewWorker("fixture", &testClient{query: func(ctx context.Context, _, _ uint64) ([]*store.EthData, error) {
				if cancelQuery {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, failure
			}}, 1)
			s := &Syncer[*store.EthData]{config: config{Benchmark: true, Boost: boostConfig{Enabled: boost, DefaultTaskSize: 1, ForcePersistenceInterval: time.Second}}, workers: []*worker[*store.EthData]{w}, elm: election.NewNoopLeaderManager()}
			err := s.SyncOnce(ctx, 1, 2)
			cancel()
			if cancelQuery {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.ErrorIs(t, err, failure)
			}
		}
	}
}

func TestWorkerStopsAtMaximumHeight(t *testing.T) {
	w := mustNewWorker("fixture", &testClient{query: func(_ context.Context, from, _ uint64) ([]*store.EthData, error) {
		return []*store.EthData{testData(from)}, nil
	}}, 1)
	require.NoError(t, w.sync(context.Background(), math.MaxUint64, math.MaxUint64, 2, true))
	require.EqualValues(t, uint64(math.MaxUint64), (<-w.Data()).Number())
}
