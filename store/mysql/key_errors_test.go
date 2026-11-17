package mysql

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMySQLDuplicateKeyDoesNotExposeCredential(t *testing.T) {
	db := policyTestDB(t, true)
	cs := newConfStore(db)
	require.NoError(t, cs.StoreConfig(RateLimitStrategyConfKeyPrefix+"default", "{}"))
	snapshot, err := cs.LoadConfigSnapshot(RateLimitStrategyConfKeyPrefix + "default")
	require.NoError(t, err)
	keys := NewRateLimitStore(db)
	const key = "0123456789abcdef0123456789abcdef"
	require.NoError(t, keys.AddRateLimit(snapshot.ID, 0, 0, key, ""))
	err = keys.AddRateLimit(snapshot.ID, 0, 0, key, "")
	require.ErrorContains(t, err, "already exists")
	require.NotContains(t, err.Error(), key)
}
