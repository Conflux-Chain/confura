//go:build integration

package sync

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
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
	"gorm.io/gorm"
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

// cacheCommitPool pauses one real COMMIT after MySQL has released its locks,
// while the original syncOnce is still waiting to update its local state.
type cacheCommitPool struct {
	gorm.ConnPool
	armed     atomic.Bool
	committed chan struct{}
	release   chan struct{}
}

func (p *cacheCommitPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	tx, err := p.ConnPool.(gorm.TxBeginner).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &cacheCommitTx{Tx: tx, pool: p}, nil
}

type cacheCommitTx struct {
	*sql.Tx
	pool *cacheCommitPool
}

func (tx *cacheCommitTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.pool.armed.CompareAndSwap(true, false) {
		close(tx.pool.committed)
		<-tx.pool.release
	}
	return nil
}

func TestMySQLSyncWaitsForLeadershipCacheReset(t *testing.T) {
	for _, tc := range []struct{ space, timing string }{
		{"Core", "before reset"}, {"Core", "after reset"},
		{"eSpace", "before reset"}, {"eSpace", "after reset"},
	} {
		space, timing := tc.space, tc.timing
		t.Run(space+"/"+timing, func(t *testing.T) {
			db, cfg := newSyncTestDB(t)
			raw, err := db.DB()
			require.NoError(t, err)
			pause := &cacheCommitPool{ConnPool: raw, committed: make(chan struct{}), release: make(chan struct{})}
			writerDB := db.Session(&gorm.Session{NewDB: true, Initialized: true})
			writerDB.Statement.ConnPool = pause
			writerDB = writerDB.Session(&gorm.Session{NewDB: true})
			var head atomic.Uint64
			head.Store(1)
			require.NoError(t, db.AutoMigrate(&dlock.Dlock{}))
			conf := election.Config{Lease: 10 * time.Second, Renew: time.Second, Retry: time.Millisecond}
			a := election.NewDlockLeaderManager(db, conf, "cache-reset")
			b := election.NewDlockLeaderManager(db, conf, "cache-reset")
			ctx := context.Background()
			var push func(election.LeaderManager, uint64, byte) error
			var pop func(election.LeaderManager) error
			var reset func(context.Context, election.LeaderManager, bool)
			var run func(context.Context) (bool, error)
			var cursor func() uint64
			var cacheSize func() uint32
			var maxEpoch func() (uint64, bool, error)
			var pivotHash func(uint64) (string, bool, error)
			if space == "Core" {
				ms := mysql.NewCfxStore(writerDB, cfg, syncTestFilter{})
				cli, err := sdk.NewClientWithProvider(&syncTestProvider{call: func(_ context.Context, o interface{}, m string, args ...interface{}) error {
					switch m {
					case "cfx_getStatus":
						*o.(*ct.Status) = ct.Status{NetworkID: 1}
					case "cfx_epochNumber":
						*o.(**hexutil.Big) = ct.NewBigInt(head.Load())
					case "cfx_getBlocksByEpoch":
						n, _ := args[0].(*ct.Epoch).ToInt()
						*o.(*[]ct.Hash) = []ct.Hash{*syncTestCore(n.Uint64(), 'A').PivotHash}
					case "cfx_getBlockByHashWithPivotAssumption":
						*o.(*ct.Block) = *syncTestCore(uint64(args[2].(hexutil.Uint64)), 'A').GetPivotBlock()
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
				reset, run, cursor, cacheSize, maxEpoch, pivotHash = s.onLeadershipChanged, s.syncOnce, func() uint64 { return s.epochFrom }, s.pivotWin.Size, ms.MaxEpoch, ms.PivotHash
			} else {
				ms := mysql.NewEthStore(writerDB, cfg, syncTestFilter{})
				cli := web3go.NewClientWithProvider(&syncTestProvider{call: func(_ context.Context, o interface{}, m string, args ...interface{}) error {
					if m != "eth_getBlockByNumber" {
						return fmt.Errorf("unexpected %s", m)
					}
					n := args[0].(et.BlockNumber)
					if n < 0 {
						n = et.BlockNumber(head.Load())
					}
					*o.(**et.Block) = syncTestEth(uint64(n), 'A').Block
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
			pause.armed.Store(true)
			oldDone := make(chan struct{})
			var oldErr error
			go func() { _, oldErr = run(ctx); close(oldDone) }()
			var releaseCommitOnce sync.Once
			releaseCommit := func() { releaseCommitOnce.Do(func() { close(pause.release) }) }
			joinOld := func() {
				select {
				case <-oldDone:
				case <-time.After(3 * time.Second):
					t.Fatal("old syncOnce did not finish")
				}
			}
			t.Cleanup(func() { releaseCommit(); joinOld() })
			select {
			case <-pause.committed:
			case <-waitCtx.Done():
				t.Fatal("old syncOnce did not commit")
			}
			n, ok, err := maxEpoch()
			require.NoError(t, err)
			require.True(t, ok)
			require.EqualValues(t, 1, n)
			// The same production syncOnce resumes after the real lease takeover.
			stopA()
			stopB := startCallbackCampaign(t, b)
			require.True(t, b.Await(waitCtx))
			require.NoError(t, pop(b))
			require.NoError(t, push(b, 1, 'B'))
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
			if timing == "before reset" {
				releaseCommit()
				joinOld()
				require.NoError(t, oldErr)
				require.Zero(t, cacheSize(), "old publication must not refill the ousted window")
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
			if timing == "after reset" {
				releaseCommit()
				joinOld()
				require.NoError(t, oldErr)
				require.Zero(t, cacheSize(), "old publication must not refill the reelected window")
			}
			head.Store(2)
			// A2 extends stale A1, but the durable head is B1. The production
			// syncOnce must read B1 and roll back, rather than persist A2 on B1.
			done, err := run(ctx)
			require.NoError(t, err)
			require.False(t, done)
			n, ok, err = maxEpoch()
			require.NoError(t, err)
			require.True(t, ok)
			t.Logf("early admission=%v, durable height=%d, cursor=%d, cache size=%d", early, n, cursor(), cacheSize())
			require.Zero(t, n, "incompatible child must not be committed")
			require.EqualValues(t, 1, cursor())
			require.Zero(t, cacheSize())
		})
	}
}
