package ratelimit

import (
	"strings"
	"testing"

	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/stretchr/testify/require"
)

func TestNormalizeLimitKey(t *testing.T) {
	for _, input := range []string{"short", strings.Repeat("a", 129), "0123456789abcdef!"} {
		key, err := normalizeLimitKey(input, rate.LimitTypeByKey)
		require.Error(t, err)
		require.Empty(t, key)
		require.NotContains(t, err.Error(), input)
	}
	valid := strings.Repeat("a", 128)
	key, err := normalizeLimitKey(" \t"+valid+" \n", rate.LimitTypeByKey)
	require.NoError(t, err)
	require.Equal(t, valid, key)
	for _, kind := range []rate.LimitType{rate.LimitTypeByKey, rate.LimitTypeByIp} {
		for _, input := range []string{"", " \t"} {
			key, err := normalizeLimitKey(input, kind)
			require.NoError(t, err)
			require.True(t, handlers.IsAccessTokenValid(key))
			require.LessOrEqual(t, len(key), 128)
		}
	}
}
