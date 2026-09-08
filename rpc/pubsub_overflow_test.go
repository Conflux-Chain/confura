package rpc

import (
	"testing"
	"time"

	rpc "github.com/openweb3/go-rpc-provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newStalledDelegateSub returns a delegate context holding one subscription whose
// downstream channel has no reader, so every deliver overflows onto the (buffered,
// size 1) error channel.
func newStalledDelegateSub(t *testing.T) (*delegateContext, *delegateSubscription) {
	t.Helper()

	dctx := newDelegateContext()
	ch := make(chan int) // unbuffered, no reader
	sub := newDelegateSubscription(dctx, rpc.NewID(), ch)
	dctx.delegateSubs.Store(sub.subId, sub)

	return dctx, sub
}

func completesWithin(d time.Duration, fn func()) bool {
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// notify must not block on a subscriber whose error channel is already full. The
// delegate is shared across all subscribers, and notify holds the read lock, so a
// blocking send here would stall delivery for everyone.
func TestNotifyDoesNotBlockOnFullErrorChannel(t *testing.T) {
	dctx, sub := newStalledDelegateSub(t)

	// First notify overflows and buffers exactly one error.
	dctx.notify(1)
	require.Len(t, sub.err, 1)

	// A second notify must complete even though the error channel is now full.
	require.True(t, completesWithin(2*time.Second, func() { dctx.notify(2) }),
		"notify blocked while a subscriber's error channel was full")

	// The first overflow error is still delivered to the subscriber.
	select {
	case err := <-sub.err:
		assert.Equal(t, rpc.ErrSubscriptionQueueOverflow, err)
	default:
		t.Fatal("expected the first overflow error to be buffered for the subscriber")
	}
}

// cancel must not block on a subscriber whose error channel is already full. It holds
// the write lock, so a blocking send here would freeze the whole delegate.
func TestCancelDoesNotBlockOnFullErrorChannel(t *testing.T) {
	dctx, sub := newStalledDelegateSub(t)

	// Pre-fill the error channel via an overflowing notify.
	dctx.notify(1)
	require.Len(t, sub.err, 1)

	require.True(t, completesWithin(2*time.Second, func() { dctx.cancel(nil) }),
		"cancel blocked while a subscriber's error channel was full")
}
