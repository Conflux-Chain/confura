package rate

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStrategyJsonUmarshal(t *testing.T) {
	// test case 1
	jsonStr1 := `{
		"rules":{"rpc.all":[100,1000],"cfx_getLogs":[20,20]}, 
		"limitType":1, 
		"keyLen":10, 
		"priority": 1, 
		"regex":"^[[:alnum:]]{10}$"
	}
	`
	strategy := Strategy{}
	err := json.Unmarshal([]byte(jsonStr1), &strategy)
	assert.Nil(t, err)
	assert.True(t, strategy.WouldLimitByKey())
	assert.True(t, strategy.KeyMust())
	assert.Equal(t, strategy.Priority, 1)
	assert.True(t, strategy.ValidateKey("dd238zdf32"))
	assert.False(t, strategy.ValidateKey("dd238zdf3249c8sv"))
	assert.False(t, strategy.ValidateKey("{d238zdf32"))

	// test case 2
	jsonStr2 := `{
		"rules":{"rpc.all":[100,1000],"cfx_getLogs":[20,20]}
	}
	`
	strategy2 := Strategy{}
	err = json.Unmarshal([]byte(jsonStr2), &strategy2)
	assert.Nil(t, err)
	assert.True(t, strategy2.WouldLimitByIp())
	assert.False(t, strategy2.KeyMust())
	assert.Equal(t, strategy2.Priority, 0)

	// test case 3
	jsonStr3 := `{
		"rules":{"rpc.all":[100,1000],"cfx_getLogs":[20,20]}, 
		"limitType":1, 
		"keyLen":0
	}
	`
	strategy3 := Strategy{}
	err = json.Unmarshal([]byte(jsonStr3), &strategy3)
	assert.NotNil(t, err)

	// test case 4
	jsonStr4 := `{
		"rules":{"rpc.all":[100,1000],"cfx_getLogs":[20]}, 
		"limitType":0, 
		"keyLen":10
	}
	`
	strategy4 := Strategy{}
	err = json.Unmarshal([]byte(jsonStr4), &strategy4)
	assert.NotNil(t, err)
}
