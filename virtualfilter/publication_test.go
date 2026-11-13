package virtualfilter

import (
	"context"
	"errors"
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
