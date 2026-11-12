//go:build integration

package sync

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/election"
	"github.com/Conflux-Chain/confura/sync/monitor"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	ct "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/dlock"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/web3go"
	et "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func startCallbackCampaign(t *testing.T, lm *election.DlockLeaderManager) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { lm.Campaign(ctx); close(done) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			require.NoError(t, lm.Stop())
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("campaign did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestMySQLSyncWaitsForLeadershipCacheReset(t *testing.T) {
	for _, space := range []string{"Core", "eSpace"} {
		t.Run(space, func(t *testing.T) {
			db, cfg := newSyncTestDB(t)
			require.NoError(t, db.AutoMigrate(&dlock.Dlock{}))
			conf := election.Config{Lease: 10 * time.Second, Renew: time.Second, Retry: time.Millisecond}
			a := election.NewDlockLeaderManager(db, conf, "cache-reset")
			b := election.NewDlockLeaderManager(db, conf, "cache-reset")
			ctx := context.Background()
			var push func(election.LeaderManager, uint64, byte) error
			var pop func(election.LeaderManager) error
			var publishOldCache func() error
			var reset func(context.Context, election.LeaderManager, bool)
			var run func(context.Context) (bool, error)
			var cursor func() uint64
			var cacheSize func() uint32
			var maxEpoch func() (uint64, bool, error)
			var pivotHash func(uint64) (string, bool, error)
			if space == "Core" {
				ms := mysql.NewCfxStore(db, cfg, syncTestFilter{})
				cli, err := sdk.NewClientWithProvider(&syncTestProvider{call: func(_ context.Context, o interface{}, m string, args ...interface{}) error {
					switch m {
					case "cfx_getStatus":
						*o.(*ct.Status) = ct.Status{NetworkID: 1}
					case "cfx_epochNumber":
						*o.(**hexutil.Big) = ct.NewBigInt(2)
					case "cfx_getBlocksByEpoch":
						*o.(*[]ct.Hash) = []ct.Hash{*syncTestCore(2, 'A').PivotHash}
					case "cfx_getBlockByHashWithPivotAssumption":
						*o.(*ct.Block) = *syncTestCore(2, 'A').GetPivotBlock()
					default:
						return fmt.Errorf("unexpected %s", m)
					}
					return nil
				}})
				require.NoError(t, err)
				s := &DatabaseSyncer{conf: &cfxSyncConfig{}, cfxs: []*sdk.Client{cli}, db: ms, maxSyncEpochs: 1, pivotWin: newCfxSlidingPivotWindow(1), elm: a, monitor: monitor.NewMonitor(monitor.NewConfig(), nil)}
				push = func(lm election.LeaderManager, n uint64, fork byte) error {
					d := syncTestCore(n, fork)
					if n == 1 {
						d.GetPivotBlock().ParentHash = syncTestCore(0, 'A').GetPivotBlock().Hash
					}
					return ms.PushnWithGuard(ctx, []*store.EpochData{d}, lm.Guard(election.WithLeadership(ctx, lm)))
				}
				pop = func(lm election.LeaderManager) error {
					return ms.PopnWithGuard(ctx, 1, lm.Guard(election.WithLeadership(ctx, lm)))
				}
				publishOldCache = func() error { return s.pivotWin.Push(cfxPivotBlockWrapper{syncTestCore(1, 'A').GetPivotBlock()}) }
				reset, run, cursor, cacheSize, maxEpoch, pivotHash = s.onLeadershipChanged, s.syncOnce, func() uint64 { return s.epochFrom }, s.pivotWin.Size, ms.MaxEpoch, ms.PivotHash
			} else {
				ms := mysql.NewEthStore(db, cfg, syncTestFilter{})
				cli := web3go.NewClientWithProvider(&syncTestProvider{call: func(_ context.Context, o interface{}, m string, _ ...interface{}) error {
					if m != "eth_getBlockByNumber" {
						return fmt.Errorf("unexpected %s", m)
					}
					*o.(**et.Block) = syncTestEth(2, 'A').Block
					return nil
				}})
				s := &EthSyncer{conf: &ethSyncConfig{}, w3cs: []*web3go.Client{cli}, db: ms, maxSyncBlocks: 1, pivotWin: newEvmSlidingPivotWindow(1), elm: a, monitor: monitor.NewMonitor(monitor.NewConfig(), nil)}
				push = func(lm election.LeaderManager, n uint64, fork byte) error {
					d := syncTestEth(n, fork)
					if n == 1 {
						d.Block.ParentHash = syncTestEth(0, 'A').Block.Hash
					}
					return ms.PushnWithGuard(ctx, []*store.EthData{d}, lm.Guard(election.WithLeadership(ctx, lm)))
				}
				pop = func(lm election.LeaderManager) error {
					return ms.PopnWithGuard(ctx, 1, lm.Guard(election.WithLeadership(ctx, lm)))
				}
				publishOldCache = func() error { return s.pivotWin.Push(evmPivotBlockWrapper{syncTestEth(1, 'A').Block}) }
				reset, run, cursor, cacheSize, maxEpoch, pivotHash = s.onLeadershipChanged, s.syncOnce, func() uint64 { return s.fromBlock }, s.pivotWin.Size, ms.MaxEpoch, ms.PivotHash
			}

			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			term := 0
			a.OnElected(func(c context.Context, _ election.LeaderManager) {
				term++
				if term == 2 {
					close(entered)
					select {
					case <-release:
					case <-c.Done():
					}
				}
			})
			a.OnElected(func(c context.Context, lm election.LeaderManager) { reset(c, lm, true) })
			a.OnOusted(func(c context.Context, lm election.LeaderManager) { reset(c, lm, false) })
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			stopA := startCallbackCampaign(t, a)
			require.True(t, a.Await(waitCtx))
			require.NoError(t, push(a, 0, 'A'))
			require.NoError(t, push(a, 1, 'A'))
			// Pause the old writer after COMMIT, before its local pivotWin.Push.
			stopA()
			stopB := startCallbackCampaign(t, b)
			require.True(t, b.Await(waitCtx))
			require.NoError(t, pop(b))
			require.NoError(t, push(b, 1, 'B'))
			require.NoError(t, publishOldCache(), "late local publication after the ousting reset")
			require.EqualValues(t, 1, cacheSize())
			stored, ok, err := pivotHash(1)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, syncTestEth(1, 'B').Block.Hash.Hex(), stored)
			stopB()
			startCallbackCampaign(t, a)
			t.Cleanup(unblock)
			select {
			case <-entered:
			case <-waitCtx.Done():
				t.Fatal("second term did not enter elected callbacks")
			}
			admitted := make(chan bool, 1)
			go func() { admitted <- a.Await(waitCtx) }()
			early := false
			select {
			case ready := <-admitted:
				require.True(t, ready)
				early = true
				t.Error("Await admitted work before the cache reset callback")
			case <-time.After(30 * time.Millisecond):
				unblock()
				require.True(t, <-admitted)
				require.Zero(t, cacheSize(), "reset must complete before admitting work")
			}
			// A2 extends stale A1, but the durable head is B1. The production
			// syncOnce must read B1 and roll back, rather than persist A2 on B1.
			done, err := run(ctx)
			require.NoError(t, err)
			require.False(t, done)
			n, ok, err := maxEpoch()
			require.NoError(t, err)
			require.True(t, ok)
			t.Logf("early admission=%v, durable height=%d, cursor=%d, cache size=%d", early, n, cursor(), cacheSize())
			require.Zero(t, n, "incompatible child must not be committed")
			require.EqualValues(t, 1, cursor())
			require.Zero(t, cacheSize())
		})
	}
}
