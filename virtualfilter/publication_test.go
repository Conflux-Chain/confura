package virtualfilter

import (
	"context"
	"encoding/json"
	"errors"
	ethtypes "github.com/openweb3/web3go/types"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	rpc "github.com/openweb3/go-rpc-provider"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type blockedObserver struct {
	pollingObserver
	entered chan struct{}
	release chan struct{}
	err     error
}

func (o *blockedObserver) onPolled(ctx context.Context, n string, id rpc.ID, c filterChanges) error {
	close(o.entered)
	select {
	case <-o.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	if o.err != nil {
		return o.err
	}
	return o.pollingObserver.onPolled(ctx, n, id, c)
}

func TestPublicationWaitsForPersistence(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		t.Run(space, func(t *testing.T) {
			_, s := newTestLogSession(t, space, 1)
			batch := s.batch(1, 2)
			s.w.client = &testPollingClient{changes: batch}
			obs := &blockedObserver{pollingObserver: s.w.observer, entered: make(chan struct{}), release: make(chan struct{})}
			s.w.observer = obs
			published := make(chan error, 1)
			go func() { published <- s.w.pollOnce() }()
			<-obs.entered
			// No reader can enter the chain view during persistence (including GC).
			if s.w.mu.TryRLock() {
				s.w.mu.RUnlock()
				t.Fatal("publication released the view before persistence")
			}
			result := make(chan filterChanges, 1)
			readErr := make(chan error, 1)
			go func() { c, e := s.f.fetch(context.Background()); result <- c; readErr <- e }()
			close(obs.release)
			require.NoError(t, <-published)
			got := <-result
			require.NoError(t, <-readErr)
			require.Equal(t, 2, logCount(got))
		})
	}
}

func TestPersistenceFailureInvalidatesBeforeUnlock(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		t.Run(space, func(t *testing.T) {
			db, s := newTestLogSession(t, space, 1)
			s.w.client = &testPollingClient{changes: s.batch(1, 2)}
			require.NoError(t, db.Callback().Create().Before("gorm:create").Register("write_fault", func(tx *gorm.DB) { tx.AddError(errors.New("write failed")) }))
			require.Error(t, s.w.pollOnce())
			_, err := s.f.fetch(context.Background())
			require.ErrorIs(t, err, errFilterNotFound)
			require.Nil(t, s.w.session.fchain)
			require.Empty(t, s.w.session.fcursors)
		})
	}
}

func TestStoreContextDoesNotAffectOtherRequests(t *testing.T) {
	_, store := testLogStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := store.WithContext(ctx).PreparePartition("0xabc")
	require.ErrorIs(t, err, context.Canceled)
	_, _, err = store.PreparePartition("0xabc")
	require.NoError(t, err)
	require.ErrorIs(t, store.WithContext(ctx).DeletePartitions("0xabc"), context.Canceled)
	require.NoError(t, store.DeletePartitions("0xabc"))
}

// The context parameter must not become an extra JSON-RPC wire argument.
func TestGetFilterChangesWireCompatibility(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		t.Run(space, func(t *testing.T) {
			_, s := newTestLogSession(t, space, 2)
			require.NoError(t, s.w.merge(s.batch(1)))
			mgr := newFilterManager()
			mgr.add(s.f)
			base := &filterSystemBase{filterMgr: mgr}
			server := rpc.NewServer()
			defer server.Stop()
			if space == "eth" {
				require.NoError(t, server.RegisterName(space, newEthFilterApi(&ethFilterSystem{filterSystemBase: base})))
			} else {
				require.NoError(t, server.RegisterName(space, newCfxFilterApi(&cfxFilterSystem{filterSystemBase: base})))
			}
			for _, size := range []int{1, 0} {
				req := httptest.NewRequest("POST", "http://fixture.invalid", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"`+space+`_getFilterChanges","params":["0x1"]}`))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				server.ServeHTTP(rec, req)
				var response struct {
					Result []json.RawMessage
					Error  json.RawMessage
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
				require.Empty(t, response.Error, rec.Body.String())
				require.Len(t, response.Result, size)
			}
		})
	}
}

func TestReaderExcludesReplayAndCleanup(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		t.Run(space, func(t *testing.T) {
			db, s := newTestLogSession(t, space, 1)
			batch := s.batch(1, 2)
			require.NoError(t, s.persist(batch))
			require.NoError(t, s.w.merge(batch))
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			require.NoError(t, db.Callback().Query().After("gorm:query").Register("hold_read", func(tx *gorm.DB) {
				if tx.Statement.Table == "vflogs_0xabc_0" {
					once.Do(func() { close(entered); <-release })
				}
			}))
			readDone := make(chan filterChanges, 1)
			readErr := make(chan error, 1)
			go func() { c, e := s.f.fetch(context.Background()); readDone <- c; readErr <- e }()
			<-entered
			// All replay, GC and closing mutations require this same exclusive lock.
			if s.w.mu.TryLock() {
				s.w.mu.Unlock()
				t.Fatal("reader released its view during materialization")
			}
			changes := filterChanges(coreRevert(0))
			if space == "eth" {
				a, b := testEthLog(1), testEthLog(2)
				a.Removed = true
				b.Removed = true
				changes = &ethtypes.FilterChanges{Logs: []ethtypes.Log{b, a}}
			}
			s.w.client = &testPollingClient{changes: changes}
			publishDone := make(chan error, 1)
			go func() { publishDone <- s.w.pollOnce() }()
			close(release)
			got := <-readDone
			require.NoError(t, <-readErr)
			wantJSON, _ := json.Marshal(batch)
			gotJSON, _ := json.Marshal(got)
			require.JSONEq(t, string(wantJSON), string(gotJSON))
			require.NoError(t, <-publishDone)
			require.NoError(t, db.Callback().Query().Remove("hold_read"))
			got, err := s.f.fetch(context.Background())
			require.NoError(t, err)
			wantJSON, _ = json.Marshal(changes)
			gotJSON, _ = json.Marshal(got)
			require.JSONEq(t, string(wantJSON), string(gotJSON))
		})
	}
}

func TestMySQLReaderExcludesPartitionGC(t *testing.T) {
	if os.Getenv("CONFURA_TEST_MYSQL_SOCKET") == "" {
		t.Skip("requires disposable local MySQL socket")
	}
	db, s := newTestLogSession(t, "eth", 1)
	publish := func(c filterChanges) { s.w.client = &testPollingClient{changes: c}; require.NoError(t, s.w.pollOnce()) }
	publish(s.batch(1, 2))
	require.NoError(t, db.Table("bn_partitions").Where("pi = ?", 0).Update("count", 10000000).Error)
	publish(s.batch(3)) // second partition, no pruning yet
	require.NoError(t, db.Table("bn_partitions").Where("pi = ?", 1).Update("count", 10000000).Error)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("gc_read", func(tx *gorm.DB) {
		if tx.Statement.Table == "vflogs_0xabc_0" {
			once.Do(func() { close(entered); <-release })
		}
	}))
	done := make(chan int, 1)
	errs := make(chan error, 1)
	go func() {
		c, e := s.f.fetch(context.Background())
		if e == nil {
			done <- logCount(c)
		} else {
			done <- -1
		}
		errs <- e
	}()
	<-entered
	s.w.client = &testPollingClient{changes: s.batch(4)}
	written := make(chan error, 1)
	go func() { written <- s.w.pollOnce() }()
	require.True(t, db.Migrator().HasTable("vflogs_0xabc_0"))
	if s.w.mu.TryLock() {
		s.w.mu.Unlock()
		t.Fatal("GC can race the active reader")
	}
	close(release)
	require.Equal(t, 3, <-done)
	require.NoError(t, <-errs)
	require.NoError(t, <-written)
	require.NoError(t, db.Callback().Query().Remove("gc_read"))
	require.False(t, db.Migrator().HasTable("vflogs_0xabc_0"))
	got, err := s.f.fetch(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, logCount(got))
}
