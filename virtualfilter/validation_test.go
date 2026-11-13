package virtualfilter

import (
	"testing"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func TestMalformedDecodeIsFatal(t *testing.T) {
	var eth *ethtypes.FilterChanges
	var cfx *cfxtypes.CfxFilterChanges
	for _, target := range []interface{}{&eth, &cfx} {
		err := decodeUpstreamLogResult([]byte(`[1]`), target)
		require.ErrorIs(t, err, errInvalidFilterChanges)
		w := testWorker(t, &testPollingClient{err: err}, newEthFilterChain(1))
		require.ErrorIs(t, w.pollOnce(), errInvalidFilterChanges, "decode errors must not enter the transport retry path")
	}
}
