package catchup

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	ct "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/web3go"
	et "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

// Compare these method sequences with the pre-validation implementation too.
// Repeated logs must reuse blocks; empty intermediate heights must not fetch them.
func TestCoreBoostRPCBudget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		to     uint64
		logged []uint64
		want   []string
	}{
		{"empty", 3, nil, []string{"logs", "members:1", "hash:11", "pivot:3"}},
		{"sparse", 3, []uint64{2}, []string{"logs", "members:1", "hash:11", "hash:22", "hash:21", "pivot:3"}},
		{"dense", 3, []uint64{1, 2, 3}, []string{"logs", "members:1", "hash:11", "hash:12", "hash:22", "hash:21", "hash:32", "hash:31", "pivot:3"}},
		{"single", 1, []uint64{1}, []string{"logs", "members:1", "hash:11", "hash:12", "pivot:1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blocks := make(map[ct.Hash]*ct.Block)
			members := make(map[uint64][]ct.Hash)
			var logs []ct.Log
			for n := uint64(1); n <= tc.to; n++ {
				for j := uint64(1); j <= 2; j++ {
					bn := n*10 + j
					bh, th := ct.Hash(fmt.Sprintf("0x%064x", bn)), ct.Hash(fmt.Sprintf("0x%064x", bn+100))
					status := hexutil.Uint64(0)
					blocks[bh] = &ct.Block{BlockHeader: ct.BlockHeader{Hash: bh, EpochNumber: ct.NewBigInt(n), BlockNumber: ct.NewBigInt(bn)}, Transactions: []ct.Transaction{{Hash: th, BlockHash: &bh, Status: &status}}}
					members[n] = append(members[n], bh)
				}
			}
			for _, n := range tc.logged {
				// Reverse block order to detect unintended output sorting.
				for j := 1; j >= 0; j-- {
					block := blocks[members[n][j]]
					for index := uint64(0); index < 3; index++ {
						logs = append(logs, ct.Log{EpochNumber: ct.NewBigInt(n), BlockHash: &block.Hash, TransactionHash: &block.Transactions[0].Hash, LogIndex: ct.NewBigInt(index), TransactionIndex: ct.NewBigInt(0)})
					}
				}
			}
			var calls []string
			client, err := sdk.NewClientWithProvider(&rpcProvider{call: func(_ context.Context, out interface{}, method string, args ...interface{}) error {
				switch method {
				case "cfx_getStatus":
					calls = append(calls, "status")
					*out.(*ct.Status) = ct.Status{NetworkID: 1}
				case "cfx_getLogs":
					calls = append(calls, "logs")
					*out.(*[]ct.Log) = logs
				case "cfx_getBlocksByEpoch", "cfx_getBlockByEpochNumber":
					n, ok := args[0].(*ct.Epoch).ToInt()
					require.True(t, ok)
					if method == "cfx_getBlocksByEpoch" {
						calls = append(calls, fmt.Sprintf("members:%d", n.Uint64()))
						*out.(*[]ct.Hash) = members[n.Uint64()]
					} else {
						require.Equal(t, true, args[1])
						calls = append(calls, fmt.Sprintf("pivot:%d", n.Uint64()))
						*out.(**ct.Block) = blocks[members[n.Uint64()][1]]
					}
				case "cfx_getBlockByHash":
					require.Equal(t, true, args[1])
					block := blocks[args[0].(ct.Hash)]
					require.NotNil(t, block)
					calls = append(calls, fmt.Sprintf("hash:%d", block.BlockNumber.ToInt().Uint64()))
					*out.(**ct.Block) = block
				default:
					return fmt.Errorf("unexpected RPC %s", method)
				}
				return nil
			}})
			require.NoError(t, err)
			data, err := NewCoreRpcClient(client).BoostQueryChainData(context.Background(), 1, tc.to)
			require.NoError(t, err)
			require.Equal(t, append([]string{"status"}, tc.want...), calls, "SDK network-ID discovery happens once")
			calls = nil
			_, err = NewCoreRpcClient(client).BoostQueryChainData(context.Background(), 1, tc.to)
			require.NoError(t, err)
			require.Equal(t, tc.want, calls, "warm client must preserve the original RPC budget")
			require.Len(t, data, int(tc.to))
			for i, d := range data {
				require.EqualValues(t, i+1, d.EpochNo)
				if d.EpochNo != tc.to {
					require.Nil(t, d.PivotHash, "preserve the original last-epoch-only pivot")
				} else {
					require.Equal(t, members[tc.to][1], *d.PivotHash)
				}
			}
			if tc.name == "dense" {
				require.Equal(t, []*ct.Block{blocks[members[2][1]], blocks[members[2][0]]}, data[1].Blocks)
				require.Len(t, data[1].Receipts, 2)
				for _, receipt := range data[1].Receipts {
					require.Len(t, receipt.Logs, 3)
				}
			}
		})
	}
}

func TestEvmBoostRPCBudget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		to     uint64
		logged []uint64
		want   []string
	}{
		{"empty", 3, nil, []string{"logs", "block:1", "block:3"}},
		{"sparse", 3, []uint64{2}, []string{"logs", "block:1", "block:2", "block:3"}},
		{"dense", 3, []uint64{1, 2, 3}, []string{"logs", "block:1", "block:2", "block:3"}},
		{"single", 1, []uint64{1}, []string{"logs", "block:1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blocks := make(map[uint64]*et.Block)
			var logs []et.Log
			for n := uint64(1); n <= tc.to; n++ {
				bh, th := common.BigToHash(new(big.Int).SetUint64(n)), common.BigToHash(new(big.Int).SetUint64(n+100))
				blocks[n] = &et.Block{Number: new(big.Int).SetUint64(n), Hash: bh, Transactions: *et.NewTxOrHashListByTxs([]et.TransactionDetail{{Hash: th}})}
			}
			for _, n := range tc.logged {
				for index := uint(0); index < 3; index++ {
					block := blocks[n]
					logs = append(logs, et.Log{BlockNumber: n, BlockHash: block.Hash, TxHash: block.Transactions.Transactions()[0].Hash, Index: index})
				}
			}
			var calls []string
			client := NewEvmRpcClient(web3go.NewClientWithProvider(&rpcProvider{call: func(_ context.Context, out interface{}, method string, args ...interface{}) error {
				switch method {
				case "eth_getLogs":
					calls = append(calls, "logs")
					*out.(*[]et.Log) = logs
				case "eth_getBlockByNumber":
					n := uint64(args[0].(et.BlockNumber))
					require.Equal(t, true, args[1])
					calls = append(calls, fmt.Sprintf("block:%d", n))
					*out.(**et.Block) = blocks[n]
				default:
					return fmt.Errorf("unexpected RPC %s", method)
				}
				return nil
			}}))
			data, err := client.BoostQueryChainData(context.Background(), 1, tc.to)
			require.NoError(t, err)
			require.Equal(t, tc.want, calls)
			require.Len(t, data, int(tc.to))
		})
	}
}
