package util

import (
	"testing"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/openweb3/web3go"
	"github.com/stretchr/testify/assert"
)

func TestGetShortIdOfHash(t *testing.T) {
	v := GetShortIdOfHash("0x255aeaf1dbc7d18feeb232f99fdca8adc72d68f5a5da9081b0315507f8005674")
	assert.NotZero(t, v)

	v = GetShortIdOfHash("0x000")
	assert.NotZero(t, v)

	v = GetShortIdOfHash("")
	assert.NotZero(t, v)

	v = GetShortIdOfHash("0x")
	assert.NotZero(t, v)
}

func TestNormalizeEthBlockNumber(t *testing.T) {
	w3c, err := web3go.NewClient("http://evmtestnet.confluxrpc.com")
	assert.NoError(t, err)

	latestBlockNum := rpc.LatestBlockNumber
	blockNum, err := NormalizeEthBlockNumber(w3c, &latestBlockNum, rpc.BlockNumber(0))
	assert.NoError(t, err)
	assert.NotNil(t, blockNum)

	pendingBlockNum := rpc.PendingBlockNumber
	blockNum, err = NormalizeEthBlockNumber(w3c, &pendingBlockNum, rpc.BlockNumber(0))
	if blockNum != nil {
		assert.NoError(t, err)
	} else {
		assert.NoError(t, err)
	}

	earlistBlockNum := rpc.EarliestBlockNumber
	blockNum, err = NormalizeEthBlockNumber(w3c, &earlistBlockNum, rpc.BlockNumber(0))
	if blockNum != nil {
		assert.NoError(t, err)
	}
}
