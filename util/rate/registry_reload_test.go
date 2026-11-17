package rate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/stretchr/testify/require"
)

func TestStartReloadWaitsForInitialConfiguration(t *testing.T) {
	r := NewRegistry(NewKeyLoader(func(*KeysetFilter) ([]*KeyInfo, error) { return nil, nil }), acl.NewEthValidator)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- r.StartReload(ctx, &wg, time.Hour, time.Second, func(context.Context) (*Config, error) {
			close(entered)
			<-release
			return &Config{Strategies: map[uint32]*Strategy{1: NewStrategy(1, DefaultStrategy)}}, nil
		})
	}()
	<-entered
	select {
	case err := <-done:
		t.Fatalf("returned before configuration was available: %v", err)
	default:
	}
	close(release)
	require.NoError(t, <-done)
	require.Len(t, r.Strategies(), 1)
}

func TestStartReloadFailureAndTimeout(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		r := NewRegistry(nil, acl.NewEthValidator)
		var wg sync.WaitGroup
		err := r.StartReload(context.Background(), &wg, time.Hour, 10*time.Millisecond, func(ctx context.Context) (*Config, error) {
			if timeout {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return nil, errors.New("database unavailable")
		})
		require.Error(t, err)
		if timeout {
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
		wg.Wait() // no periodic task was registered after a failed initialization
	}
}

func TestReloadRetainsConfigurationAndCancelsQuery(t *testing.T) {
	r := NewRegistry(nil, acl.NewEthValidator)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	var calls atomic.Int32
	failed, blocked := make(chan struct{}), make(chan struct{})
	require.NoError(t, r.StartReload(ctx, &wg, time.Millisecond, time.Second, func(ctx context.Context) (*Config, error) {
		switch calls.Add(1) {
		case 1:
			return &Config{Strategies: map[uint32]*Strategy{1: NewStrategy(1, DefaultStrategy)}}, nil
		case 2:
			close(failed)
			return nil, errors.New("reload failed")
		default:
			close(blocked)
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}))
	<-failed
	<-blocked
	require.Len(t, r.Strategies(), 1)
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reload query did not exit on cancellation")
	}
	require.EqualValues(t, 3, calls.Load())
}
