package rate

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshalStrategy(t *testing.T) {
	stgJsonStr := `{
	"rulesets":[
		{
			"algo": "time_window",
			"rules": {"rpc_all":[100000,86400]}
		},
		{
			"algo": "token_bucket",
			"rules": {"rpc_all":[100,1000],"cfx_call":[10,200]}
		}
	]
}`

	var stg StrategyV2
	err := json.Unmarshal(([]byte)(stgJsonStr), &stg)
	assert.NoError(t, err)
	assert.Equal(t, len(stg.RuleSets), 2)

	assert.Equal(t, stg.RuleSets[0], NewTimeWindowRuleSet(map[string]TimeWindowOption{
		"rpc_all": {
			Quota:    100000,
			Interval: 86400 * time.Second,
		},
	}))

	assert.Equal(t, stg.RuleSets[1], NewTokenBucketRuleSet(map[string]TokenBucketOption{
		"rpc_all":  NewTokenBucketOption(100, 1000),
		"cfx_call": NewTokenBucketOption(10, 200),
	}))
}
