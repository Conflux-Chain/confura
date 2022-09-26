package rate

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/assert"
)

func TestGenerateRandomLimitKey(t *testing.T) {
	// limit type by IP
	limitKey, err := GenerateRandomLimitKey(LimitTypeByIp)
	assert.Nil(t, err)
	assert.Equal(t, len(limitKey), LimitKeyLength*2)

	kbytes, err := hexutil.Decode(fmt.Sprintf("0x%v", limitKey))
	assert.Nil(t, err)

	assert.True(t, ((kbytes[0]>>4)&0x0F) == uint8(LimitTypeByIp))

	// limit type by Key
	limitKey, err = GenerateRandomLimitKey(LimitTypeByKey)
	assert.Nil(t, err)
	assert.Equal(t, len(limitKey), LimitKeyLength*2)

	kbytes, err = hexutil.Decode(fmt.Sprintf("0x%v", limitKey))
	assert.Nil(t, err)

	assert.True(t, ((kbytes[0]>>4)&0x0F) == uint8(LimitTypeByKey))

	// invalid limit type
	_, err = GenerateRandomLimitKey(-1)
	assert.NotNil(t, err)
}
