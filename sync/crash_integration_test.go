//go:build integration

package sync

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/sync/election"
	"github.com/Conflux-Chain/confura/sync/monitor"
	"github.com/Conflux-Chain/go-conflux-util/dlock"
	"github.com/openweb3/web3go"
	et "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func crashTestLeader(t *testing.T, db *gorm.DB, ctx context.Context) election.LeaderManager {
	t.Helper()
	lm := election.NewDlockLeaderManager(db, election.Config{Lease: 2 * time.Second, Renew: 500 * time.Millisecond, Retry: 10 * time.Millisecond}, "sync.crash")
	go lm.Campaign(ctx)
	require.True(t, lm.Await(ctx))
	return lm
}

func TestSyncCrashChild(t *testing.T) {
	mode := os.Getenv("CONFURA_TEST_SYNC_CRASH_MODE")
	if mode == "" {
		t.Skip("started only by the parent crash test")
	}
	db := openSyncTestDB(t, os.Getenv("CONFURA_TEST_SYNC_CRASH_DB"))
	cfg := &mysql.Config{Database: os.Getenv("CONFURA_TEST_SYNC_CRASH_DB"), AddressIndexedLogPartitions: 2, TopicIndexedLogPartitions: 2}
	ms := mysql.NewEthStore(db, cfg, syncTestFilter{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lm := crashTestLeader(t, db, ctx)
	ctx = election.WithLeadership(ctx, lm)
	if mode == "before" {
		require.NoError(t, db.Callback().Create().After("gorm:create").Register("test:exit", func(tx *gorm.DB) {
			if tx.Statement.Table == "epoch_block_map" {
				os.Exit(77)
			}
		}))
	}
	require.NoError(t, ms.PushnWithGuard(ctx, []*store.EthData{syncTestEth(4, 'A')}, lm.Guard(ctx)))
	os.Exit(78) // COMMIT completed; cursor/window updates and deferred cleanup never run.
}

func TestMySQLSyncRecoversAfterClientCrash(t *testing.T) {
	for _, mode := range []string{"before", "after"} {
		t.Run(mode, func(t *testing.T) {
			db, cfg := newSyncTestDB(t)
			require.NoError(t, db.Migrator().CreateTable(&dlock.Dlock{}))
			ms := mysql.NewEthStore(db, cfg, syncTestFilter{})
			seedSyncEth(t, ms)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSyncCrashChild$")
			cmd.Env = append(os.Environ(), "CONFURA_TEST_SYNC_CRASH_MODE="+mode, "CONFURA_TEST_SYNC_CRASH_DB="+cfg.Database)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(output))
			expectedCode, expectedMax := 77, uint64(3)
			if mode == "after" {
				expectedCode, expectedMax = 78, 4
			}
			require.Equal(t, expectedCode, exit.ExitCode(), string(output))
			// Reopen through a new connection. This tests client-process failure, not
			// a server crash or an ambiguous COMMIT acknowledgement.
			recoveredDB := openSyncTestDB(t, cfg.Database)
			recovered := mysql.NewEthStore(recoveredDB, cfg, syncTestFilter{})
			n, ok, err := recovered.MaxEpoch()
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, expectedMax, n)
			var blocks int64
			require.NoError(t, recoveredDB.Table("blocks").Count(&blocks).Error)
			require.EqualValues(t, expectedMax+1, blocks)
			logs, err := recovered.GetLogs(ctx, store.LogFilter{BlockFrom: 1, BlockTo: 5})
			require.NoError(t, err)
			require.Len(t, logs, 3)
			lm := crashTestLeader(t, recoveredDB, ctx) // Must wait for the dead writer's lease.
			defer lm.Stop()
			cli := web3go.NewClientWithProvider(&syncTestProvider{call: func(_ context.Context, o interface{}, m string, a ...interface{}) error {
				if m != "eth_getBlockByNumber" {
					return fmt.Errorf("unexpected %s", m)
				}
				n := a[0].(et.BlockNumber)
				if n < 0 {
					n = 5
				}
				*o.(**et.Block) = syncTestEth(uint64(n), 'A').Block
				return nil
			}})
			s := &EthSyncer{conf: &ethSyncConfig{}, w3cs: []*web3go.Client{cli}, db: recovered, maxSyncBlocks: 1, pivotWin: newEvmSlidingPivotWindow(1), elm: lm, monitor: monitor.NewMonitor(monitor.NewConfig(), nil)}
			require.NoError(t, s.loadLastSyncBlock())
			require.Equal(t, expectedMax+1, s.fromBlock)
			for s.fromBlock <= 5 {
				_, err = s.syncOnce(ctx)
				require.NoError(t, err)
				require.NoError(t, ctx.Err())
			}
			n, _, err = recovered.MaxEpoch()
			require.NoError(t, err)
			require.EqualValues(t, 5, n)
			logs, err = recovered.GetLogs(ctx, store.LogFilter{BlockFrom: 1, BlockTo: 5})
			require.NoError(t, err)
			require.Len(t, logs, 3)
		})
	}
}
