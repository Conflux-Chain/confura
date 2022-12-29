package rate

import (
	"crypto/md5"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshalStrategy(t *testing.T) {
	stgJsonStr := `{
	"rulesets":[
		{
			"algo":1,
			"rules": [100000,86400]
		},
		{
			"algo":2,
			"rules": {"rpc_all":[100,1000],"cfx_call":[10,200]}
		}
	]
}`

	var stg StrategyV2
	err := json.Unmarshal(([]byte)(stgJsonStr), &stg)
	assert.NoError(t, err)
	assert.Equal(t, len(stg.RuleSets), 2)

	assert.Equal(t, stg.RuleSets[0], NewTimeWindowRuleSet(TimeWindowOption{
		Quota:    100000,
		Interval: 86400 * time.Second,
	}))

	assert.Equal(t, stg.RuleSets[1], NewTokenBucketRuleSet(map[string]TokenBucketOption{
		"rpc_all":  NewTokenBucketOption(100, 1000),
		"cfx_call": NewTokenBucketOption(10, 200),
	}))

	assert.Equal(t, stg.MD5, md5.Sum(([]byte)(stgJsonStr)))
}
