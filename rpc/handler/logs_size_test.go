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

func TestLogArraySizeAcrossSources(t *testing.T) {
	for _, batches := range [][][]json.RawMessage{
		nil,
		{{}},
		{{json.RawMessage(`{"source":"fullnode"}`), json.RawMessage(`{"source":"fullnode"}`)}},
		{{json.RawMessage(`{"source":"db"}`)}, {}, {json.RawMessage(`{"source":"archive"}`)}, {json.RawMessage(`{"source":"fullnode"}`)}},
	} {
		var size logArraySize
		all := make([]json.RawMessage, 0)
		for _, batch := range batches {
			for _, log := range batch {
				size.add(uint64(len(log)))
				all = append(all, log)
			}
			want, err := json.Marshal(all)
			require.NoError(t, err)
			require.EqualValues(t, len(want), size.size())
		}
		want, err := json.Marshal(all)
		require.NoError(t, err)
		require.EqualValues(t, len(want), size.size())
	}
}

func FuzzLogJSONSizeMatchesEncoding(f *testing.F) {
	f.Add(uint64(0), []byte{}, "", uint16(0))
	f.Add(uint64(math.MaxUint64), []byte{0, 255}, "<trace>\n\"\\\x00\u2028\u2029世界\xff", uint16(0xffff))
	f.Add(uint64(16), []byte{1}, "core", uint16(0x5555))
	f.Fuzz(func(t *testing.T, value uint64, data []byte, text string, flags uint16) {
		// Bound each input so fuzzing explores field combinations, not huge allocations.
		if len(data) > 2048 || len(text) > 256 {
			return
		}
		index := uint(value)
		eth := web3types.Log{BlockNumber: value, BlockTimestamp: value, Index: index, TxIndex: index, Data: data, Removed: flags&1 != 0}
		if flags&2 != 0 {
			eth.LogType = &text
		}
		if flags&4 != 0 {
			eth.TransactionLogIndex = &index
		}
		if flags&8 != 0 {
			eth.Topics = make([]common.Hash, (flags>>4)%5)
		}
		want, err := json.Marshal(eth)
		require.NoError(t, err)
		require.EqualValues(t, len(want), ethLogJSONSize(&eth))

		quantity := new(big.Int).SetUint64(value)
		quantity.Lsh(quantity, uint(flags%193)) // Include quantities wider than 64 bits.
		if flags&16 != 0 {
			quantity.Neg(quantity)
		}
		q := (*hexutil.Big)(quantity)
		hash := cfxtypes.Hash(common.BytesToHash(data).Hex())
		space := cfxtypes.SpaceType(text)
		core := cfxtypes.Log{Address: cfxaddress.MustNewFromHex("0x1000000000000000000000000000000000000001", uint32(flags)+1), Data: data}
		if flags&1 != 0 {
			core.BlockHash = &hash
		}
		if flags&2 != 0 {
			core.TransactionHash = &hash
		}
		if flags&4 != 0 {
			core.Space = &space
		}
		if flags&8 != 0 {
			core.Topics = make([]cfxtypes.Hash, 0)
			for i := uint16(0); i < (flags>>4)%5; i++ {
				core.Topics = append(core.Topics, hash)
			}
		}
		fields := []**hexutil.Big{&core.EpochNumber, &core.TransactionIndex, &core.LogIndex, &core.TransactionLogIndex, &core.BlockTimestamp}
		for i, field := range fields {
			if flags&(1<<uint(i+8)) != 0 {
				*field = q
			}
		}
		want, err = json.Marshal(core)
		require.NoError(t, err)
		require.EqualValues(t, len(want), cfxLogJSONSize(&core))
		stringJSON, err := json.Marshal(text)
		require.NoError(t, err)
		require.Equal(t, len(stringJSON), jsonStringSize(text))
	})
}
