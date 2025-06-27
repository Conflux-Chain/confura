package util

import (
	"testing"

	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGetShortIdOfHash(t *testing.T) {
	v := GetShortIdOfHash("0x255aeaf1dbc7d18feeb232f99fdca8adc72d68f5a5da9081b0315507f8005674")
	assert.NotZero(t, v)
}

// mockBlockNumberResolver is a mock implementation of EthBlockNumberResolver
type mockBlockNumberResolver struct {
	mock.Mock
}

func (m *mockBlockNumberResolver) Resolve(bn types.BlockNumber) (uint64, error) {
	args := m.Called(bn)
	return args.Get(0).(uint64), args.Error(1)
}

func TestNormalizeEthBlockNumber(t *testing.T) {
	mockResolver := new(mockBlockNumberResolver)
	hardfork := types.BlockNumber(100)

	t.Run("nil blockNum", func(t *testing.T) {
		_, err := NormalizeEthBlockNumber(mockResolver, nil, hardfork)
		assert.ErrorContains(t, err, "block number must be provided")
	})

	t.Run("blockNum > hardfork", func(t *testing.T) {
		bn := types.BlockNumber(101)
		out, err := NormalizeEthBlockNumber(mockResolver, &bn, hardfork)
		assert.NoError(t, err)
		assert.Equal(t, &bn, out)
	})

	t.Run("blockNum > 0 but <= hardfork", func(t *testing.T) {
		bn := types.BlockNumber(50)
		out, err := NormalizeEthBlockNumber(mockResolver, &bn, hardfork)
		assert.NoError(t, err)
		assert.Equal(t, &hardfork, out)
	})

	t.Run("blockNum == earliest", func(t *testing.T) {
		bn := types.EarliestBlockNumber
		out, err := NormalizeEthBlockNumber(mockResolver, &bn, hardfork)
		assert.NoError(t, err)
		assert.Equal(t, &hardfork, out)
	})

	t.Run("blockNum == latest, mock resolve success", func(t *testing.T) {
		bn := types.LatestBlockNumber
		mockResolver.On("Resolve", bn).Return(uint64(150), nil).Once()

		out, err := NormalizeEthBlockNumber(mockResolver, &bn, hardfork)
		assert.NoError(t, err)
		assert.Equal(t, types.BlockNumber(150), *out)
	})

	t.Run("blockNum == pending, mock resolve error", func(t *testing.T) {
		bn := types.PendingBlockNumber
		mockResolver.On("Resolve", bn).Return(uint64(0), errors.New("resolve failed")).Once()

		out, err := NormalizeEthBlockNumber(mockResolver, &bn, hardfork)
		assert.Nil(t, out)
		assert.ErrorContains(t, err, "failed to get block")
	})
}
