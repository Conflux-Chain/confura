package election

import (
	"context"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/require"
)

func TestLeadershipCallbacks(t *testing.T) {
	lm := NewDlockLeaderManager(nil, Config{}, "test")
	var events []LeaderStatus
	lm.OnElected(func(context.Context, LeaderManager) {
		require.Equal(t, StatusElected, lm.electionStatus.Load())
		events = append(events, StatusElected)
	})
	lm.OnOusted(func(context.Context, LeaderManager) {
		require.Equal(t, StatusOusted, lm.electionStatus.Load())
		events = append(events, StatusOusted)
	})
	for range 2 {
		lm.onElected(context.Background())
		lm.onOusted(context.Background())
	}
	require.Equal(t, []LeaderStatus{StatusElected, StatusOusted, StatusElected, StatusOusted}, events)
}

func TestGuardRejectsWorkCapturedDuringElectedCallbacks(t *testing.T) {
	lm := NewDlockLeaderManager(nil, Config{}, "test")
	lm.nonce.Store("new-term")
	var captured context.Context
	lm.OnElected(func(context.Context, LeaderManager) {
		captured = WithLeadership(context.Background(), lm)
	})
	lm.onElected(context.Background())
	// Even after callbacks finish, this work must not inherit readiness it did
	// not have when it captured the new term. Rejection precedes any DB access.
	require.True(t, lm.electedCallbacksDone.Load())
	require.ErrorIs(t, lm.Guard(captured)(nil), store.ErrLeaderRenewal)
}

func awaitResult(lm LeaderManager, ctx context.Context) <-chan bool {
	done := make(chan bool, 1)
	go func() { done <- lm.Await(ctx) }()
	return done
}

func requireAwaitBlocked(t *testing.T, done <-chan bool) {
	t.Helper()
	select {
	case ready := <-done:
		t.Fatalf("Await returned %v before leadership was ready", ready)
	case <-time.After(30 * time.Millisecond):
	}
}

func requireAwaitResult(t *testing.T, done <-chan bool, want bool) {
	t.Helper()
	select {
	case ready := <-done:
		require.Equal(t, want, ready)
	case <-time.After(time.Second):
		t.Fatal("Await did not return")
	}
}

func TestAwaitWaitsForAllElectedCallbacks(t *testing.T) {
	lm := NewDlockLeaderManager(nil, Config{Retry: time.Millisecond}, "test")
	lm.lockExpiryTime.Store(time.Now().Add(time.Minute).Unix())
	entered := make(chan LeaderStatus, 2)
	release := make(chan struct{})
	for range 2 {
		lm.OnElected(func(context.Context, LeaderManager) {
			entered <- lm.electionStatus.Load()
			<-release
		})
	}
	// Reusing the manager must not reuse the previous term's readiness.
	for range 2 {
		callbackDone := make(chan struct{})
		go func() { lm.onElected(context.Background()); close(callbackDone) }()
		require.Equal(t, StatusElected, <-entered)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		waiting := awaitResult(lm, ctx)
		requireAwaitBlocked(t, waiting)
		// Cancellation must not wait for the callback mutex.
		canceledCtx, cancelWaiter := context.WithCancel(context.Background())
		canceled := awaitResult(lm, canceledCtx)
		cancelWaiter()
		requireAwaitResult(t, canceled, false)
		release <- struct{}{}
		require.Equal(t, StatusElected, <-entered)
		requireAwaitBlocked(t, waiting)
		release <- struct{}{}
		<-callbackDone
		requireAwaitResult(t, waiting, true)
		lm.onOusted(context.Background())
		require.False(t, lm.electedCallbacksDone.Load())
		waiting = awaitResult(lm, ctx)
		requireAwaitBlocked(t, waiting)
		cancel()
		requireAwaitResult(t, waiting, false)
	}
}

func TestAwaitRejectsCanceledCallbacksOrExpiredLease(t *testing.T) {
	for _, cancelCallbacks := range []bool{true, false} {
		t.Run(map[bool]string{true: "canceled callbacks", false: "expired lease"}[cancelCallbacks], func(t *testing.T) {
			lm := NewDlockLeaderManager(nil, Config{Retry: time.Millisecond}, "test")
			lm.lockExpiryTime.Store(time.Now().Add(time.Minute).Unix())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			lm.OnElected(func(context.Context, LeaderManager) {
				if cancelCallbacks {
					cancel()
				} else {
					lm.lockExpiryTime.Store(time.Now().Add(-time.Second).Unix())
				}
			})
			lm.onElected(ctx)
			waitCtx, cancelWaiter := context.WithCancel(context.Background())
			defer cancelWaiter()
			waiting := awaitResult(lm, waitCtx)
			requireAwaitBlocked(t, waiting)
			cancelWaiter()
			requireAwaitResult(t, waiting, false)
		})
	}
}
