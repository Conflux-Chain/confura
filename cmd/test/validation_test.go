package test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidatorParameters(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		require.Error(t, validateIntervals(interval, time.Second))
		require.Error(t, validateIntervals(time.Second, interval))
	}
	require.NoError(t, validateIntervals(time.Second, time.Second))
	for _, endpoint := range []string{"", "localhost:8545", "http://", "file:///tmp/rpc", "ws://localhost:8545"} {
		require.Error(t, validateEndpoints(false, endpoint), endpoint)
	}
	require.Error(t, validateEndpoints(true, "https://localhost:8545"))
	require.NoError(t, validateEndpoints(false, "http://localhost:8545", "https://localhost/rpc"))
	require.NoError(t, validateEndpoints(true, "ws://localhost:8545", "wss://localhost/rpc"))
	require.Error(t, validateNetwork("typo"))
	require.NoError(t, validateNetwork("CFX"))
	require.NoError(t, validateNetwork("ETH"))
}
