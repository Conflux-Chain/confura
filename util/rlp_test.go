package util

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/nsf/jsondiff"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var (
	mFullnode = MustNewCfxClient(
		"http://main.confluxrpc.com",
		WithClientRetryCount(3),
		WithClientRetryInterval(time.Second),
		WithClientRequestTimeout(5*time.Second),
	)
	tFullnode = MustNewCfxClient(
		"http://test.confluxrpc.com",
		WithClientRetryCount(3),
		WithClientRetryInterval(time.Second),
		WithClientRequestTimeout(5*time.Second),
	)
)

func TestTransactionRLPMarshal(t *testing.T) {
	testCases := []struct {
		txHash types.Hash
		cfx    sdk.ClientOperator
	}{
		{types.Hash("0xdeffba5291f0d669ca63514f2f9c95fd313fcf6c2855ec929076225890113b59"), tFullnode},
		{types.Hash("0x4016c5b1675182700ef67b9df90c13ddf2e774b12385af63ba43576039b13f8a"), mFullnode},
		{types.Hash("0xb50a9350d407a1d2fa0c58dea2cd1d5d406ca23023779f6af1b223fe5482e518"), mFullnode},
	}

	for i, tc := range testCases {
		fmt.Println("Running test case #", i+1, "for TestTransactionRLPMarshal")

		ptx, err := tc.cfx.GetTransactionByHash(tc.txHash)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to get transaction from fullnode")
		}

		var tx2 types.Transaction
		testRlpMarshalInOut(t, *ptx, &tx2)

		testJsonMarshalEqual(t, *ptx, tx2)
	}
}

func TestTransactionReceiptRLPMarshal(t *testing.T) {
	testCases := []struct {
		txHash types.Hash
		cfx    sdk.ClientOperator
	}{
		{types.Hash("0xdeffba5291f0d669ca63514f2f9c95fd313fcf6c2855ec929076225890113b59"), tFullnode},
		{types.Hash("0xa2c678cc97e07ce060b71f87ac65e68d482abf8e1a93b7d1bc425504c4584ca7"), mFullnode},
		{types.Hash("0x2ceccc75b871d50f90cb819b758eec179d6b1ff0a435297359c33ba96ec21135"), mFullnode},
	}

	for i, tc := range testCases {
		fmt.Println("Running test case #", i+1, "for TestTransactionReceiptRLPMarshal")

		prcpt, err := tc.cfx.GetTransactionReceipt(tc.txHash)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to get transaction receipt from fullnode")
		}

		var rcpt2 types.TransactionReceipt
		testRlpMarshalInOut(t, *prcpt, &rcpt2)

		testJsonMarshalEqual(t, *prcpt, rcpt2)
	}
}

func TestBlockSummaryRLPMarshal(t *testing.T) {
	testBlockHashes := []types.Hash{
		types.Hash("0xa6528367a9287ed3a66fc64457db15e2aaa93104a3fd06d4f0a2beb6cc1f26c8"),
		types.Hash("0xf2855662d53e36d32bece4e8a3ac7bd8368dd721c8b3f1749fcabc0d1c25d698"),
	}

	for i, blockHash := range testBlockHashes {
		fmt.Println("Running test case #", i+1, "for TestBlockSummaryRLPMarshal")

		pbs, err := mFullnode.GetBlockSummaryByHash(blockHash)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to get block summary from fullnode")
		}

		var bs2 types.BlockSummary
		testRlpMarshalInOut(t, *pbs, &bs2)

		testJsonMarshalEqual(t, *pbs, bs2)
	}
}

func TestBlockRLPMarshal(t *testing.T) {
	testBlockHashes := []types.Hash{
		types.Hash("0x11b5c88b4e42fcf95cb1454d5de03d7f31fb59f80df1e49c0723f4f86516ef01"),
		types.Hash("0x26f15dc6f353485cdfb1b370becc4abfdacbd36e39c3f9f42be724fe4073cfeb"),
	}

	for i, blockHash := range testBlockHashes {
		fmt.Println("Running test case #", i+1, "for TestBlockRLPMarshal")

		pb, err := mFullnode.GetBlockByHash(blockHash)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to get block from fullnode")
		}

		var b2 types.Block
		testRlpMarshalInOut(t, *pb, &b2)

		testJsonMarshalEqual(t, *pb, b2)
	}
}

func testRlpMarshalInOut(t *testing.T, in, out interface{}) {
	// RLP marshal input to bytes
	dBytes, err := rlp.EncodeToBytes(in)
	assert.NoError(t, err)
	assert.True(t, len(dBytes) > 0)

	// RLP unmarshal bytes output
	err = rlp.DecodeBytes(dBytes, out)
	assert.NoError(t, err)
}

func testJsonMarshalEqual(t *testing.T, v1, v2 interface{}) {
	// Json marshal v1
	jBytes1, err := json.Marshal(v1)
	if err != nil {
		logrus.WithField("value", v1).WithError(err).Fatal("Failed to json marshal v1")
	}
	txJsonStr := string(jBytes1)

	// Json marshal v2
	jBytes2, err := json.Marshal(v2)
	if err != nil {
		logrus.WithField("value", v2).WithError(err).Fatal("Failed to json marshal v2")
	}
	txJsonStr2 := string(jBytes2)

	diffOpt := jsondiff.DefaultJSONOptions()
	_, diff := jsondiff.Compare(jBytes1, jBytes2, &diffOpt)
	fmt.Printf("Json diff compared for both v1 and v2 %v\n", diff)

	assert.Equal(t, txJsonStr, txJsonStr2)
}
