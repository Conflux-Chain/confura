package virtualfilter

import (
	"context"
	"runtime"
	"testing"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func TestCapacityCountsForksBeforeMutation(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		t.Run(space, func(t *testing.T) {
			_, s := newTestLogSession(t, space, 1)
			var base *filterChainBase
			if space == "eth" {
				base = s.w.session.fchain.(*ethFilterChain).filterChainBase
			} else {
				base = s.w.session.fchain.(*cfxFilterChain).filterChainBase
			}
			base.maxNodes = 3
			require.NoError(t, s.w.merge(s.batch(1, 2)))
			if space == "eth" {
				l := testEthLog(2)
				l.Removed = true
				require.NoError(t, s.w.merge(&ethtypes.FilterChanges{Logs: []ethtypes.Log{l}}))
			} else {
				require.NoError(t, s.w.merge(coreRevert(1)))
			}
			require.NoError(t, s.w.merge(s.batch(3)))
			require.Equal(t, 2, base.len)
			require.Len(t, base.hashToNodes, 3)
			before := base.snapshotLatestCursor()
			require.ErrorIs(t, s.w.merge(s.batch(4, 5, 6)), errFilterChainCapacity)
			require.Equal(t, before, base.snapshotLatestCursor())
			require.Len(t, base.hashToNodes, 3)
			// A full budget still permits reusing retained hashes.
			require.NoError(t, s.w.session.fchain.checkCapacity(s.batch(2)))
			factory := func() filterChain { return newEthFilterChain(1) }
			if space == "cfx" {
				factory = func() filterChain { return newCfxFilterChain(1) }
			}
			s.w.client = &testPollingClient{changes: s.batch(4), chain: factory}
			require.ErrorIs(t, s.w.pollOnce(), errFilterChainCapacity)
			_, err := s.f.fetch(context.Background())
			require.ErrorIs(t, err, errFilterNotFound)
			require.NotNil(t, s.w.closing)
			s.w.retryCleanup(context.Background(), true)
			require.Nil(t, s.w.closing)
			require.NoError(t, s.w.accept(s.f))
			got, err := s.f.fetch(context.Background())
			require.NoError(t, err)
			require.Zero(t, logCount(got))
			// Stop the lifecycle goroutine registered by accept.
			s.w.close()
			s.w.retryCleanup(context.Background(), true)
			s.w.shutdownCtx.Wg.Wait()
		})
	}
}

func TestRemovedPayloadsStayInCacheBudget(t *testing.T) {
	_, s := newTestLogSession(t, "eth", 1)
	normal := s.batch(1, 2, 3)
	require.NoError(t, s.persist(normal))
	require.NoError(t, s.w.merge(normal))
	// Consumer sees the original canonical logs, including DB cache misses.
	got, err := s.f.fetch(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, logCount(got))
	removed := []ethtypes.Log{testEthLog(3), testEthLog(2), testEthLog(1)}
	for i := range removed {
		removed[i].Removed = true
	}
	changes := &ethtypes.FilterChanges{Logs: removed}
	s.w.client = &testPollingClient{changes: changes}
	require.NoError(t, s.w.pollOnce())
	chain := s.w.session.fchain.(*ethFilterChain)
	payloads := 0
	for _, node := range chain.hashToNodes {
		if len(node.filterNodable.(*ethFilterBlock).logs) > 0 {
			payloads++
		}
	}
	require.Equal(t, 1, payloads)
	require.Equal(t, 1, chain.fullBlockhashCache.Len())
	got, err = s.f.fetch(context.Background())
	require.NoError(t, err)
	require.Equal(t, removed, got.(*ethtypes.FilterChanges).Logs)
	got, err = s.f.fetch(context.Background())
	require.NoError(t, err)
	require.Zero(t, logCount(got))
}

func TestCoreInitialRevertUsesNodeBudget(t *testing.T) {
	chain := newCfxFilterChain(1)
	chain.maxNodes = 1
	require.NoError(t, chain.merge(coreRevert(10)))
	require.ErrorIs(t, chain.merge(&cfxtypes.CfxFilterChanges{Logs: []*cfxtypes.SubscriptionLog{testCfxLog(11)}}), errFilterChainCapacity)
}

func BenchmarkFilterHistory(b *testing.B) {
	logs := make([]ethtypes.Log, defaultMaxFilterChainNodes)
	for i := range logs {
		logs[i] = testEthLog(uint64(i + 1))
	}
	changes := &ethtypes.FilterChanges{Logs: logs}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ReportAllocs()
	b.ResetTimer()
	var chain *ethFilterChain
	for i := 0; i < b.N; i++ {
		chain = newEthFilterChain(100)
		if err := chain.merge(changes); err != nil {
			b.Fatal(err)
		}
		if len(chain.hashToNodes) != defaultMaxFilterChainNodes {
			b.Fatal("history count")
		}
	}
	b.StopTimer()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(changes)
	runtime.KeepAlive(chain)
	if after.HeapAlloc > before.HeapAlloc {
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc), "retained-B/session")
	}

}
