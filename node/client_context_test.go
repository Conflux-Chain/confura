package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Signal after the registration entry check has observed a live context.
type registrationContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (c *registrationContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.checked) })
	return err
}

func awaitRegistrationSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("client registration did not reach the controlled boundary")
	}
}

func TestClientRegistrationCancellationAfterEntryCheck(t *testing.T) {
	for _, sameNode := range []bool{false, true} {
		name := "new client"
		if sameNode {
			name = "cached after waiting"
		}
		t.Run(name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			var factories atomic.Int32
			provider := newClientProvider[string](nil, nil, GroupEthHttp, func(_ Group, url string) (string, error) {
				if factories.Add(1) == 1 {
					close(entered)
					<-release
				}
				return url, nil
			})
			const firstURL = "http://127.0.0.1:19001"
			secondURL := "http://127.0.0.1:19002"
			if sameNode {
				secondURL = firstURL
			}
			first := make(chan error, 1)
			go func() {
				_, err := provider.getOrRegisterClient(context.Background(), firstURL, GroupEthHttp)
				first <- err
			}()
			awaitRegistrationSignal(t, entered)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			observed := &registrationContext{Context: ctx, checked: make(chan struct{})}
			type result struct {
				client string
				err    error
			}
			second := make(chan result, 1)
			go func() {
				client, err := provider.getOrRegisterClient(observed, secondURL, GroupEthHttp)
				second <- result{client, err}
			}()
			awaitRegistrationSignal(t, observed.checked)
			// The entry check captured nil; the group lock remains held by the first factory.
			cancel()
			unblock()
			require.NoError(t, <-first)
			select {
			case result := <-second:
				require.ErrorIs(t, result.err, context.Canceled)
				require.Empty(t, result.client)
			case <-time.After(time.Second):
				t.Fatal("canceled registration did not finish after releasing the lock")
			}
			require.EqualValues(t, 1, factories.Load(), "a canceled waiter must not start a factory")
			client, err := provider.getOrRegisterClient(context.Background(), firstURL, GroupEthHttp)
			require.NoError(t, err)
			require.Equal(t, firstURL, client)
			require.EqualValues(t, 1, factories.Load())
		})
	}
}

func TestClientRegistrationKeepsCompletedClientAfterCancellation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var factories atomic.Int32
	provider := newClientProvider[string](nil, routerFunc(func(context.Context, Group, []byte) (string, error) {
		return "http://127.0.0.1:19001", nil
	}), GroupEthHttp, func(_ Group, url string) (string, error) {
		factories.Add(1)
		close(entered)
		<-release
		return url, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		client, err := provider.GetClientContext(ctx, "fixture")
		if client != "" {
			done <- errors.New("canceled request received a client")
			return
		}
		done <- err
	}()
	awaitRegistrationSignal(t, entered)
	cancel()
	unblock()
	require.ErrorIs(t, <-done, context.Canceled)
	client, err := provider.GetClient("fixture")
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:19001", client)
	require.EqualValues(t, 1, factories.Load())
}

func TestClientRegistrationDeadlineAndFactoryFailure(t *testing.T) {
	failure := errors.New("fixture factory failure")
	var factories atomic.Int32
	provider := newClientProvider[string](nil, nil, GroupEthHttp, func(Group, string) (string, error) {
		factories.Add(1)
		return "", failure
	})
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := provider.getOrRegisterClient(ctx, "fixture", GroupEthHttp)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, factories.Load())
	for i := 0; i < 2; i++ {
		_, err = provider.getOrRegisterClient(context.Background(), "fixture", GroupEthHttp)
		require.ErrorIs(t, err, failure)
	}
	require.EqualValues(t, 2, factories.Load(), "failed clients must not enter the cache")
}
