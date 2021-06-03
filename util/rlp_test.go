package util

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/nsf/jsondiff"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var (
	fullnode = MustNewCfxClientWithRetry("http://main.confluxrpc.com", 3, time.Second)
	txHash   = types.Hash("0x4016c5b1675182700ef67b9df90c13ddf2e774b12385af63ba43576039b13f8a")
)

func TestTransactionRLPMarshal(t *testing.T) {
	ptx, err := fullnode.GetTransactionByHash(txHash)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get transaction from fullnode")
	}

	// RLP marshal tx to bytes
	dBytes, err := rlp.EncodeToBytes(*ptx)
	assert.NoError(t, err)
	assert.True(t, len(dBytes) > 0)

	// RLP unmarshal bytes to a new transaction
	var tx2 types.Transaction
	err = rlp.DecodeBytes(dBytes, &tx2)
	assert.NoError(t, err)

	// Json marshal tx
	jBytes1, err := json.Marshal(*ptx)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to json marshal for tx")
	}
	txJsonStr := string(jBytes1)

	// Json marshal tx2
	jBytes2, err := json.Marshal(tx2)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to json marshal for tx2")
	}
	txJsonStr2 := string(jBytes2)

	diffOpt := jsondiff.DefaultJSONOptions()
	_, diff := jsondiff.Compare(jBytes1, jBytes2, &diffOpt)
	fmt.Printf("Json diff compared for both tx and tx2 %v\n", diff)

	assert.Equal(t, txJsonStr, txJsonStr2)
}
