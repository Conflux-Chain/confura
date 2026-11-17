package relay

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestRelayConfigurationFailureReturnsError(t *testing.T) {
	defer viper.Reset()
	for _, invalid := range []interface{}{-1, "invalid"} {
		viper.Reset()
		viper.Set("relay.bufferSize", invalid)
		r, err := NewTxnRelayerFromViper(false)
		require.Error(t, err)
		require.Nil(t, r)
	}
	viper.Reset()
	r, err := NewTxnRelayerFromViper(true)
	require.NoError(t, err)
	r.Close()
	r.Close()
}
