package virtualfilter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store/mysql"
	rpc "github.com/openweb3/go-rpc-provider"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type resourceClient struct {
	created, uninstalled atomic.Int32
	uninstallFails       atomic.Bool
}

func (c *resourceClient) establish(context.Context) (pollingSession, error) {
	c.created.Add(1)
	return *newPollingSession(rpc.NewID(), newEthFilterChain(2)), nil
}
func (c *resourceClient) fetch(context.Context, rpc.ID) (filterChanges, error) {
	return &ethtypes.FilterChanges{}, nil
}
func (c *resourceClient) uninstall(context.Context, rpc.ID) (bool, error) {
	c.uninstalled.Add(1)
	if c.uninstallFails.Load() {
		return false, errors.New("offline")
	}
	return true, nil
}

type cleanupObserver struct {
	store             *mysql.VirtualFilterLogStore
	fail, prepareFail atomic.Bool
	attempts          atomic.Int32
	attempted         chan struct{}
}

func (o *cleanupObserver) onEstablished(ctx context.Context, _ string, id rpc.ID) error {
	_, _, err := o.store.WithContext(ctx).PreparePartition(string(id))
	if err != nil {
		return err
	}
	if o.prepareFail.Load() {
		return errors.New("prepare failed after allocation")
	}
	return nil
}
func (o *cleanupObserver) onPolled(context.Context, string, rpc.ID, filterChanges) error { return nil }
func (o *cleanupObserver) onClosed(ctx context.Context, _ string, id rpc.ID) error {
	o.attempts.Add(1)
	if o.attempted != nil {
		select {
		case o.attempted <- struct{}{}:
		default:
		}
	}
	if o.fail.Load() {
		return errors.New("DB offline")
	}
	return o.store.WithContext(ctx).DeletePartitions(string(id))
}
func waitWorker(t *testing.T, w *filterWorker, predicate func() bool) {
	t.Helper()
	require.Eventually(t, func() bool { w.mu.RLock(); defer w.mu.RUnlock(); return predicate() }, 6*time.Second, 10*time.Millisecond)
}

func TestCleanupRetriesWithoutRequests(t *testing.T) {
	db, store := testLogStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	c := &resourceClient{}
	o := &cleanupObserver{store: store, attempted: make(chan struct{}, 10)}
	w := newFilterWorker("eth", "fixture", c, o, cmdutil.GracefulShutdownContext{Ctx: ctx, Wg: wg})
	f := newEthFilter("0x1", filterTypeLog, nil)
	require.NoError(t, w.accept(f))
	o.fail.Store(true)
	_, err := w.reject(f)
	require.NoError(t, err)
	select {
	case <-o.attempted:
	case <-time.After(4 * time.Second):
		t.Fatal("cleanup did not start")
	}
	waitWorker(t, w, func() bool { return w.closing != nil && !w.closing.busy })
	require.ErrorIs(t, w.accept(f), errFilterCleanupPending)
	require.EqualValues(t, 1, c.created.Load())
	require.EqualValues(t, 1, c.uninstalled.Load())
	var count int64
	require.NoError(t, db.Table("bn_partitions").Count(&count).Error)
	require.EqualValues(t, 1, count)
	o.fail.Store(false)
	// No new accept/poll request triggers the recovery.
	waitWorker(t, w, func() bool { return w.closing == nil })
	require.NoError(t, db.Table("bn_partitions").Count(&count).Error)
	require.Zero(t, count)
	require.EqualValues(t, 1, c.uninstalled.Load())
	require.NoError(t, w.accept(f))
	require.EqualValues(t, 2, c.created.Load())
	cancel()
	wg.Wait()
	require.EqualValues(t, 2, c.uninstalled.Load())
	require.NoError(t, db.Table("bn_partitions").Count(&count).Error)
	require.Zero(t, count)
	w.mu.RLock()
	defer w.mu.RUnlock()
	require.False(t, w.running)
	require.Nil(t, w.closing)
}

func TestFailedCreationRetainsCleanup(t *testing.T) {
	_, store := testLogStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	c := &resourceClient{}
	c.uninstallFails.Store(true)
	o := &cleanupObserver{store: store}
	o.prepareFail.Store(true)
	w := newFilterWorker("eth", "fixture", c, o, cmdutil.GracefulShutdownContext{Ctx: ctx, Wg: wg})
	f := newEthFilter("0x1", filterTypeLog, nil)
	require.ErrorContains(t, w.accept(f), "prepare failed")
	w.retryCleanup(ctx, true)
	require.ErrorIs(t, w.accept(f), errFilterCleanupPending)
	require.EqualValues(t, 1, o.attempts.Load())
	c.uninstallFails.Store(false)
	w.retryCleanup(ctx, true)
	require.EqualValues(t, 1, o.attempts.Load(), "completed DB step must not repeat")
	cancel()
	wg.Wait()
	require.Nil(t, w.closing)
}

func TestTimeoutLoopStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	newFilterSystemBase(time.Hour, nil, cmdutil.GracefulShutdownContext{Ctx: ctx, Wg: wg})
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout loop leaked")
	}
}

func TestMalformedSessionDoesNotStopOtherWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	shutdown := cmdutil.GracefulShutdownContext{Ctx: ctx, Wg: wg}
	bad := newFilterWorker("eth", "bad", &testPollingClient{changes: (*ethtypes.FilterChanges)(nil)}, nil, shutdown)
	good := newFilterWorker("eth", "good", &testPollingClient{changes: &ethtypes.FilterChanges{}}, nil, shutdown)
	require.NoError(t, bad.accept(newEthFilter("bad", filterTypeLog, nil)))
	require.NoError(t, good.accept(newEthFilter("good", filterTypeLog, nil)))
	waitWorker(t, bad, func() bool { return bad.session.fid == nilRpcId && bad.closing == nil })
	good.mu.RLock()
	require.NotEqual(t, nilRpcId, good.session.fid)
	require.Len(t, good.session.fcursors, 1)
	good.mu.RUnlock()
	cancel()
	wg.Wait()
}
