package v3

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshalStrategy(t *testing.T) {
	stgJsonStr := `{
		"rpc_all_qps": { 
			"algo": "token_bucket", 
			"option": {"rate": 100, "burst":1000}
		},
		"rpc_all_daily": {
			"algo": "fixed_window", 
			"option": {"interval": "24h", "quota":100000}
		}
	}`

	stg := NewStrategy(1, "default")

	err := json.Unmarshal(([]byte)(stgJsonStr), &stg)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(stg.LimitOptions))

	assert.Equal(t, NewTokenBucketOption(100, 1000), stg.LimitOptions["rpc_all_qps"])

	fwopt := FixedWindowOption{Interval: 24 * time.Hour, Quota: 100000}
	assert.Equal(t, fwopt, stg.LimitOptions["rpc_all_daily"])
}
