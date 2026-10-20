package handler

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func TestLogJSONSizeMatchesEncoding(t *testing.T) {
	for _, value := range []uint64{0, 15, 16, 255, 256, math.MaxUint64} {
		for _, dataSize := range []int{0, 1, 1024} {
			kind := "<trace>\n\"\\\x00\u2028\u2029世界\xff"
			index := uint(value)
			eth := web3types.Log{BlockNumber: value, BlockTimestamp: value, Index: index, TxIndex: index, TransactionLogIndex: &index, Data: make([]byte, dataSize), LogType: &kind, Removed: true, Topics: []common.Hash{{1}, {2}, {3}, {4}}}
			want, err := json.Marshal(eth)
			require.NoError(t, err)
			require.EqualValues(t, len(want), ethLogJSONSize(&eth))
			dbEth, err := store.ParseEthLog(&eth).ToEthLog()
			require.NoError(t, err)
			dbJSON, err := json.Marshal(dbEth)
			require.NoError(t, err)
			require.EqualValues(t, len(dbJSON), ethLogJSONSize(dbEth))
			for _, topics := range [][]common.Hash{nil, {}} {
				eth.Topics = topics
				eth.LogType = nil
				eth.TransactionLogIndex = nil
				eth.Removed = false
				want, err = json.Marshal(eth)
				require.NoError(t, err)
				require.EqualValues(t, len(want), ethLogJSONSize(&eth))
			}
		}
	}
	oldConfig := cfxaddress.GetConfig()
	t.Cleanup(func() { cfxaddress.SetConfig(oldConfig) })
	for _, verbose := range []bool{false, true} {
		cfxaddress.SetConfig(cfxaddress.Config{AddressStringVerbose: verbose})
		for _, network := range []uint32{1, 1029, 123456789} {
			for _, value := range []uint64{0, 16, 256, math.MaxUint64} {
				quantity := (*hexutil.Big)(new(big.Int).SetUint64(value))
				hash := cfxtypes.Hash(common.HexToHash("0x1234").Hex())
				space := cfxtypes.SpaceType("core")
				cfx := cfxtypes.Log{Address: cfxaddress.MustNewFromHex("0x1000000000000000000000000000000000000001", network), Topics: []cfxtypes.Hash{hash, hash}, Data: make([]byte, 1024), BlockHash: &hash, TransactionHash: &hash, EpochNumber: quantity, TransactionIndex: quantity, LogIndex: quantity, TransactionLogIndex: quantity, BlockTimestamp: quantity, Space: &space}
				want, err := json.Marshal(cfx)
				require.NoError(t, err)
				require.EqualValues(t, len(want), cfxLogJSONSize(&cfx))
				// Store conversion does not preserve space; compare against its actual JSON.
				converted, err := store.ParseCfxLog(&cfx, 10).ToCfxLog()
				require.NoError(t, err)
				want, err = json.Marshal(converted)
				require.NoError(t, err)
				require.EqualValues(t, len(want), cfxLogJSONSize(converted))
				for _, topics := range [][]cfxtypes.Hash{nil, {}} {
					cfx = cfxtypes.Log{Address: cfx.Address, Topics: topics}
					want, err = json.Marshal(cfx)
					require.NoError(t, err)
					require.EqualValues(t, len(want), cfxLogJSONSize(&cfx))
				}
			}
		}
	}
}

var logSizeBenchmarkResult uint64

func BenchmarkLogJSONSize(b *testing.B) {
	for _, size := range []int{0, 1024, 1 << 20} {
		eth := web3types.Log{Data: make([]byte, size), Topics: []common.Hash{{1}, {2}}}
		cfx := cfxtypes.Log{Address: cfxaddress.MustNewFromHex("0x1000000000000000000000000000000000000001", 1029), Data: make([]byte, size), Topics: []cfxtypes.Hash{cfxtypes.Hash(common.Hash{}.Hex())}, EpochNumber: cfxtypes.NewBigInt(16)}
		b.Run(fmt.Sprintf("eth/estimate/%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				logSizeBenchmarkResult = ethLogJSONSize(&eth)
			}
		})
		b.Run(fmt.Sprintf("core/estimate/%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				logSizeBenchmarkResult = cfxLogJSONSize(&cfx)
			}
		})
		b.Run(fmt.Sprintf("eth/marshal/%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, _ := json.Marshal(&eth)
				logSizeBenchmarkResult = uint64(len(data))
			}
		})
		b.Run(fmt.Sprintf("core/marshal/%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, _ := json.Marshal(&cfx)
				logSizeBenchmarkResult = uint64(len(data))
			}
		})

	}
}
