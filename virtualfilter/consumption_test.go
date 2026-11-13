package virtualfilter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/big"
	"sync"
	"testing"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/store/mysql"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/mattn/go-sqlite3"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// All data are local fixtures; no fullnode, credentials, or repository config.
func init() {
	sql.Register("vf_test_sqlite", &sqlite3.SQLiteDriver{ConnectHook: func(c *sqlite3.SQLiteConn) error {
		if err := c.RegisterFunc("LEAST", func(a, b int64) int64 {
			if a < b {
				return a
			}
			return b
		}, true); err != nil {
			return err
		}
		return c.RegisterFunc("GREATEST", func(a, b int64) int64 {
			if a > b {
				return a
			}
			return b
		}, true)
	}})
}
func testLogStore(t *testing.T) (*gorm.DB, *mysql.VirtualFilterLogStore) {
	t.Helper()
	db, err := gorm.Open(sqlite.Dialector{DriverName: "vf_test_sqlite", DSN: t.TempDir() + "/filter.db?_journal_mode=WAL&_busy_timeout=5000"}, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	d, err := db.DB()
	require.NoError(t, err)
	d.SetMaxOpenConns(4)
	t.Cleanup(func() { d.Close() })
	require.NoError(t, db.Exec(`CREATE TABLE bn_partitions (id INTEGER PRIMARY KEY, entity TEXT, pi INTEGER, count INTEGER, bn_min INTEGER, bn_max INTEGER, created_at datetime, updated_at datetime)`).Error)
	return db, mysql.NewVirtualFilterLogStore(db)
}
func testEthLog(n uint64) ethtypes.Log {
	return ethtypes.Log{BlockNumber: n, BlockHash: common.BigToHash(newBig(n)), TxHash: common.BigToHash(newBig(n + 100)), Data: []byte{byte(n)}}
}
func newBig(n uint64) *big.Int { return new(big.Int).SetUint64(n) }
func testCfxLog(n uint64) *cfxtypes.SubscriptionLog {
	h := cfxtypes.Hash(common.BigToHash(newBig(n)).Hex())
	z := hexutil.Big(*newBig(0))
	e := hexutil.Big(*newBig(n))
	return &cfxtypes.SubscriptionLog{Log: &cfxtypes.Log{Address: cfxaddress.MustNewFromBase32("cfxtest:aaejuaaaaaaaaaaaaaaaaaaaaaaaaaaaajh3dw3ctn"), EpochNumber: &e, BlockHash: &h, LogIndex: &z}}
}

type testLogSession struct {
	w       *filterWorker
	f       virtualFilter
	persist func(filterChanges) error
	batch   func(...uint64) filterChanges
}

func newTestLogSession(t *testing.T, space string, cache int) (*gorm.DB, *testLogSession) {
	t.Helper()
	db, store := testLogStore(t)
	s := &testLogSession{}
	if space == "eth" {
		w := &ethFilterWorker{filterWorker: testWorker(t, &testPollingClient{}, newEthFilterChain(cache))}
		f := &ethLogFilter{ethFilter: newEthFilter("0x1", filterTypeLog, &node.Web3goClient{URL: "http://fixture.invalid"}), worker: w, logStore: store}
		fs := &ethFilterSystem{filterSystemBase: &filterSystemBase{logStore: store}}
		s.w, s.f = w.filterWorker, f
		s.w.observer = fs
		s.persist = func(c filterChanges) error { return fs.onPolled(context.Background(), "fixture", "0xabc", c) }
		s.batch = func(ns ...uint64) filterChanges {
			ls := []ethtypes.Log{}
			for _, n := range ns {
				ls = append(ls, testEthLog(n))
			}
			return &ethtypes.FilterChanges{Logs: ls}
		}
	} else {
		w := &cfxFilterWorker{filterWorker: testWorker(t, &testPollingClient{}, newCfxFilterChain(cache))}
		f := &cfxLogFilter{cfxFilter: newCfxFilter("0x1", filterTypeLog, &sdk.Client{}), worker: w, logStore: store}
		fs := &cfxFilterSystem{filterSystemBase: &filterSystemBase{logStore: store}}
		s.w, s.f = w.filterWorker, f
		s.w.observer = fs
		s.persist = func(c filterChanges) error { return fs.onPolled(context.Background(), "fixture", "0xabc", c) }
		s.batch = func(ns ...uint64) filterChanges {
			ls := []*cfxtypes.SubscriptionLog{}
			for _, n := range ns {
				ls = append(ls, testCfxLog(n))
			}
			return &cfxtypes.CfxFilterChanges{Type: "log", Logs: ls}
		}
	}
	s.w.session.fcursors[s.f.fid()] = &filterDelegate{}
	return db, s
}
func logCount(c filterChanges) int {
	if e, ok := c.(*ethtypes.FilterChanges); ok {
		return len(e.Logs)
	}
	return len(c.(*cfxtypes.CfxFilterChanges).Logs)
}
func TestReadFailureDoesNotConsume(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		t.Run(space, func(t *testing.T) {
			for _, fault := range []string{"query", "decode", "cancel"} {
				t.Run(fault, func(t *testing.T) {
					db, s := newTestLogSession(t, space, 1)
					batch := s.batch(1, 2)
					require.NoError(t, s.persist(batch))
					require.NoError(t, s.w.merge(batch))
					if fault == "query" {
						require.NoError(t, db.Callback().Query().Before("gorm:query").Register("read_fault", func(tx *gorm.DB) { tx.AddError(errors.New("read failed")) }))
					}
					if fault == "decode" {
						require.NoError(t, db.Table("vflogs_0xabc_0").Where("bn = ?", 1).Update("json_repr", []byte("bad json")).Error)
					}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if fault == "cancel" {
						cancel()
					}
					_, err := s.f.fetch(ctx)
					require.Error(t, err)
					require.Equal(t, nilFilterCursor, s.w.session.fcursors[s.f.fid()].cursor)
					if fault == "query" {
						require.NoError(t, db.Callback().Query().Remove("read_fault"))
					}
					if fault == "decode" {
						require.NoError(t, s.persist(batch))
					}
					got, err := s.f.fetch(context.Background())
					require.NoError(t, err)
					want, _ := json.Marshal(batch)
					actual, _ := json.Marshal(got)
					require.JSONEq(t, string(want), string(actual))
					got, err = s.f.fetch(context.Background())
					require.NoError(t, err)
					require.Zero(t, logCount(got))
				})
			}
		})
	}
}
func TestConcurrentConsumptionAndReject(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		t.Run(space, func(t *testing.T) {
			_, s := newTestLogSession(t, space, 10)
			require.NoError(t, s.w.merge(s.batch(1, 2, 3)))
			start := make(chan struct{})
			results := make(chan int, 2)
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					c, e := s.f.fetch(context.Background())
					if e != nil {
						results <- -1
					} else {
						results <- logCount(c)
					}
				}()
			}
			close(start)
			wg.Wait()
			require.Equal(t, 3, <-results+<-results)
			d, unlock, err := s.w.beginRead(context.Background(), s.f.fid())
			require.NoError(t, err)
			require.NotNil(t, d)
			rejected := make(chan struct{})
			go func() { s.w.reject(s.f); close(rejected) }()
			unlock()
			<-rejected
			_, err = s.f.fetch(context.Background())
			require.ErrorIs(t, err, errFilterNotFound)
		})
	}
}

func TestDelegateReadOwnership(t *testing.T) {
	_, s := newTestLogSession(t, "eth", 10)
	s.w.session.fcursors["0x2"] = &filterDelegate{}
	_, unlock, err := s.w.beginRead(context.Background(), "0x1")
	require.NoError(t, err)
	// Different delegates can hold the same chain view concurrently.
	_, otherUnlock, err := s.w.beginRead(context.Background(), "0x2")
	require.NoError(t, err)
	otherUnlock()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, release, e := s.w.beginRead(ctx, "0x1")
		if release != nil {
			release()
		}
		done <- e
	}()
	<-started
	cancel()
	unlock()
	require.ErrorIs(t, <-done, context.Canceled)
}
