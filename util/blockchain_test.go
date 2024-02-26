package util

import (
	"testing"

	"github.com/openweb3/web3go"
	w3types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/assert"
)

func TestGetShortIdOfHash(t *testing.T) {
	v := GetShortIdOfHash("0x255aeaf1dbc7d18feeb232f99fdca8adc72d68f5a5da9081b0315507f8005674")
	assert.NotZero(t, v)
}

func TestNormalizeEthBlockNumber(t *testing.T) {
	w3c, err := web3go.NewClient("http://evmtestnet.confluxrpc.com")
	assert.NoError(t, err)

	latestBlockNum := w3types.LatestBlockNumber
	blockNum, err := NormalizeEthBlockNumber(w3c, &latestBlockNum, w3types.BlockNumber(0))
	assert.NoError(t, err)
	assert.NotNil(t, blockNum)

	pendingBlockNum := w3types.PendingBlockNumber
	_, err = NormalizeEthBlockNumber(w3c, &pendingBlockNum, w3types.BlockNumber(0))
	assert.NoError(t, err)

	earlistBlockNum := w3types.EarliestBlockNumber
	blockNum, err = NormalizeEthBlockNumber(w3c, &earlistBlockNum, w3types.BlockNumber(0))
	if blockNum != nil {
		assert.NoError(t, err)
	}
}
