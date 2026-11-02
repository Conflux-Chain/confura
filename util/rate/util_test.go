package rate

import (
	"bytes"
	"crypto/rand"
	"errors"
	"github.com/btcsuite/btcutil/base58"
	"github.com/stretchr/testify/require"
	"testing"
)

type failingRandomReader struct{}

func (failingRandomReader) Read([]byte) (int, error) { return 0, errors.New("entropy unavailable") }
func TestGenerateRandomLimitKey(t *testing.T) {
	original := rand.Reader
	t.Cleanup(func() { rand.Reader = original })
	for _, kind := range []LimitType{LimitTypeByKey, LimitTypeByIp} {
		entropy := bytes.Repeat([]byte{42}, LimitKeyLength)
		rand.Reader = bytes.NewReader(entropy)
		key, err := GenerateRandomLimitKey(kind)
		require.NoError(t, err)
		expected := []byte(base58.Encode(entropy))
		expected[0] = '0' + byte(kind)
		require.Equal(t, string(expected), key)
	}
	rand.Reader = failingRandomReader{}
	key, err := GenerateRandomLimitKey(LimitTypeByKey)
	require.ErrorContains(t, err, "entropy unavailable")
	require.Empty(t, key)
	_, err = GenerateRandomLimitKey(LimitType(255))
	require.Error(t, err)
}
