package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubSubLimiterAcquireConnection(t *testing.T) {
	limiter := NewPubSubLimiter(PubSubLimitConfig{
		MaxConnections:      2,
		MaxConnectionsPerIP: 1,
	})

	session1, err := limiter.AcquireConnection("1.1.1.1")
	require.NoError(t, err)
	require.NotNil(t, session1)

	_, err = limiter.AcquireConnection("1.1.1.1")
	require.Error(t, err)

	session2, err := limiter.AcquireConnection("2.2.2.2")
	require.NoError(t, err)
	require.NotNil(t, session2)

	_, err = limiter.AcquireConnection("3.3.3.3")
	require.Error(t, err)

	session1.Close()
	session1.Close()

	session3, err := limiter.AcquireConnection("1.1.1.1")
	require.NoError(t, err)
	require.NotNil(t, session3)

	session2.Close()
	session3.Close()

	stats := limiter.Stats()
	assert.Equal(t, 0, stats.Connections)
	assert.Empty(t, stats.ConnectionsByIP)
}

func TestPubSubLimiterAcquireSubscription(t *testing.T) {
	limiter := NewPubSubLimiter(PubSubLimitConfig{
		MaxConnections:                10,
		MaxConnectionsPerIP:           10,
		MaxSubscriptions:              3,
		MaxSubscriptionsPerIP:         2,
		MaxSubscriptionsPerConnection: 2,
	})

	session1, err := limiter.AcquireConnection("1.1.1.1")
	require.NoError(t, err)
	session2, err := limiter.AcquireConnection("1.1.1.1")
	require.NoError(t, err)
	session3, err := limiter.AcquireConnection("2.2.2.2")
	require.NoError(t, err)

	permit1, err := session1.AcquireSubscription()
	require.NoError(t, err)
	permit2, err := session1.AcquireSubscription()
	require.NoError(t, err)

	_, err = session1.AcquireSubscription()
	require.Error(t, err)

	_, err = session2.AcquireSubscription()
	require.Error(t, err)

	permit3, err := session3.AcquireSubscription()
	require.NoError(t, err)

	_, err = session3.AcquireSubscription()
	require.Error(t, err)

	permit1.Release()
	permit1.Release()

	stats := limiter.Stats()
	assert.Equal(t, 2, stats.Subscriptions)
	assert.Equal(t, 1, stats.SubscriptionsByIP["1.1.1.1"])
	assert.Equal(t, 1, stats.SubscriptionsByIP["2.2.2.2"])

	session1.Close()
	session2.Close()
	session3.Close()

	permit2.Release()
	permit3.Release()

	stats = limiter.Stats()
	assert.Equal(t, 0, stats.Connections)
	assert.Equal(t, 0, stats.Subscriptions)
	assert.Empty(t, stats.ConnectionsByIP)
	assert.Empty(t, stats.SubscriptionsByIP)
}

func TestPubSubSessionContext(t *testing.T) {
	limiter := NewPubSubLimiter(PubSubLimitConfig{})
	session, err := limiter.AcquireConnection("1.1.1.1")
	require.NoError(t, err)

	ctx := ContextWithPubSubSession(context.Background(), session)
	got, ok := PubSubSessionFromContext(ctx)
	require.True(t, ok)
	assert.Same(t, session, got)
}
