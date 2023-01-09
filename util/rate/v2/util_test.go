package v2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateRandomLimitKey(t *testing.T) {
	// limit type by IP
	limitKey, err := GenerateRandomLimitKey(LimitTypeByIp)
	assert.Nil(t, err)

	assert.Greater(t, len(limitKey), LimitKeyLength)
	assert.Less(t, len(limitKey), LimitKeyLength*2)

	t.Logf("New random limit key of type `byIP`: %v", limitKey)
	assert.Equal(t, limitKey[0], uint8('0'+LimitTypeByIp))

	// limit type by Key
	limitKey, err = GenerateRandomLimitKey(LimitTypeByKey)
	assert.Nil(t, err)

	assert.Greater(t, len(limitKey), LimitKeyLength)
	assert.Less(t, len(limitKey), LimitKeyLength*2)

	t.Logf("New random limit key of type `byKey`: %v", limitKey)
	assert.Equal(t, limitKey[0], uint8('0'+LimitTypeByKey))

	// invalid limit type
	_, err = GenerateRandomLimitKey(-1)
	assert.NotNil(t, err)
}
