package catchup

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	et "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type testClient struct {
	query func(context.Context, uint64, uint64) ([]*store.EthData, error)
}

func (c *testClient) QueryChainData(ctx context.Context, a, b uint64) ([]*store.EthData, error) {
	return c.query(ctx, a, b)
}
func (c *testClient) BoostQueryChainData(ctx context.Context, a, b uint64) ([]*store.EthData, error) {
	return c.query(ctx, a, b)
}
func (*testClient) GetFinalizationStatus(context.Context) (*FinalizationStatus, error) {
	return &FinalizationStatus{}, nil
}
func (*testClient) Space() string { return "eth" }
func (*testClient) Close()        {}
func testData(n uint64) *store.EthData {
	return &store.EthData{BlockNo: n, Block: &et.Block{Number: new(big.Int).SetUint64(n)}}
}

func TestWorkerRejectsInvalidSingleton(t *testing.T) {
	for _, bad := range [][]*store.EthData{nil, {nil}, {testData(2)}, {testData(1), testData(2)}} {
		t.Run("invalid", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			w := mustNewWorker("test", &testClient{query: func(context.Context, uint64, uint64) ([]*store.EthData, error) {
				calls++
				if calls == 1 {
					return bad, nil
				}
				return []*store.EthData{testData(1)}, nil
			}}, 1)
			var wg sync.WaitGroup
			wg.Add(1)
			go w.Sync(ctx, &wg, 1, 1, 1)
			select {
			case d := <-w.Data():
				require.EqualValues(t, 1, d.Number())
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not recover")
			}
			wg.Wait()
			require.Equal(t, 2, calls)
		})
	}
}

func TestCollectorRejectsWrongEpoch(t *testing.T) {
	w := mustNewWorker("test", &testClient{}, 1)
	w.resultChan <- testData(2)
	s := &Syncer[*store.EthData]{workers: []*worker[*store.EthData]{w}}
	require.Error(t, s.fetchResult(context.Background(), 1, 1, nil))
}
