package rpc

import (
	"context"
	"encoding/json"
	"math"
	"math/big"
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/nearhead"
	cacheRpc "github.com/Conflux-Chain/confura-data-cache/rpc"
	ct "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/evm"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openweb3/go-rpc-provider/interfaces"
	"github.com/openweb3/web3go/client"
	wt "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type fixtureProvider struct {
	interfaces.Provider
	call func(context.Context, interface{}, string, ...interface{}) error
}

func (p *fixtureProvider) CallContext(ctx context.Context, out interface{}, method string, args ...interface{}) error {
	return p.call(ctx, out, method, args...)
}

type fixtureResolver struct{}

func (fixtureResolver) Resolve(n wt.BlockNumber) (uint64, error) {
	if n >= 0 {
		return uint64(n), nil
	}
	return 100, nil
}
func fixtureNearhead() *EthNearHeadCache {
	return &EthNearHeadCache{EthCache: nearhead.NewEthCache(nearhead.Config{MaxMemory: 1 << 20, MaxLogs: 100})}
}
func fixtureBlock(n int64, h common.Hash, topics []common.Hash) evm.BlockData {
	tx := common.HexToHash("0x1")
	return evm.BlockData{Block: &wt.Block{Number: big.NewInt(n), Difficulty: big.NewInt(0), Hash: h, Transactions: *wt.NewTxOrHashListByTxs([]wt.TransactionDetail{{Hash: tx}})}, Receipts: []*wt.Receipt{{TransactionHash: tx, BlockHash: h, Logs: []*wt.Log{{BlockNumber: uint64(n), BlockHash: h, TxHash: tx, Topics: topics}}}}}
}

type fixtureIndexCache struct {
	cacheRpc.Interface
	calls int
}

func (c *fixtureIndexCache) GetTransactionByIndex(_ ct.BlockHashOrNumber, i uint32) (ct.Lazy[*wt.TransactionDetail], error) {
	c.calls++
	return ct.NewLazy(&wt.TransactionDetail{Hash: common.HexToHash("0xa")})
}

func TestLargeTransactionIndexBypassesNarrowCache(t *testing.T) {
	d := &fixtureIndexCache{Interface: cacheRpc.NotFoundImpl}
	p := &fixtureProvider{call: func(_ context.Context, out interface{}, method string, args ...interface{}) error {
		require.Equal(t, hexutil.Uint(uint64(1)<<32), args[1])
		return json.Unmarshal([]byte("null"), out)
	}}
	n := fixtureNearhead()
	require.NoError(t, n.Put(fixtureBlock(100, common.Hash{}, nil)))
	c := ethCacheClient{RpcEthClient: client.NewRpcEthClient(p), cacheClientContext: cacheClientContext{nearhead: n, dataCache: d, resolver: fixtureResolver{}}}
	a, err := c.LazyTransactionByBlockHashAndIndex(common.Hash{}, uint(uint64(1)<<32))
	require.NoError(t, err)
	require.True(t, a.IsEmptyOrNull())
	b, err := c.LazyTransactionByBlockNumberAndIndex(1, uint(uint64(1)<<32))
	require.NoError(t, err)
	require.True(t, b.IsEmptyOrNull())
	require.Zero(t, d.calls)
}

type fixtureTraceCache struct {
	cacheRpc.Interface
	indexes   []uint
	bulkCalls int
}

func (c *fixtureTraceCache) GetTrace(_ common.Hash, index uint) (ct.Lazy[*wt.LocalizedTrace], error) {
	c.indexes = append(c.indexes, index)
	// Model the pinned gRPC client's uint32 wire conversion. The caller must
	// skip this API when the original index does not fit.
	index = uint(uint32(index))
	traces := fixtureTransactionTraces()
	if index >= uint(len(traces)) {
		return ct.NewLazy((*wt.LocalizedTrace)(nil))
	}
	return ct.NewLazy(&traces[index])
}

func (c *fixtureTraceCache) GetTransactionTraces(common.Hash) (ct.Lazy[[]wt.LocalizedTrace], error) {
	c.bulkCalls++
	return ct.NewLazy(fixtureTransactionTraces())
}

func fixtureTransactionTraces() []wt.LocalizedTrace {
	var traces []wt.LocalizedTrace
	txHash := common.HexToHash("0x1")
	for _, path := range [][]uint{{}, {0}, {0, 1}, {0, 2}} {
		var trace wt.LocalizedTrace
		_ = json.Unmarshal([]byte(`{"type":"call","action":{"from":"0x0000000000000000000000000000000000000000","to":"0x0000000000000000000000000000000000000000","gas":"0x0","input":"0x","value":"0x0","callType":"call"},"traceAddress":[],"subtraces":0,"blockNumber":100,"blockHash":"0x0000000000000000000000000000000000000000000000000000000000000001"}`), &trace)
		trace.TraceAddress = path
		trace.TransactionHash = &txHash
		traces = append(traces, trace)
	}
	return traces
}

// Conflux eSpace selects the first flat index and returns null for empty input:
// https://github.com/Conflux-Chain/conflux-rust/blob/2cb16583060d7a569271adb59cc1a9cd92dfd447/crates/rpc/rpc-eth-impl/src/trace.rs#L305-L319
func TestTraceGetMatchesConfluxSemantics(t *testing.T) {
	traces, txHash := fixtureTransactionTraces(), common.HexToHash("0x1")
	for _, source := range []string{"nearhead", "data-cache", "fullnode"} {
		t.Run(source, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				indexes []uint
				want    int // -1 means null
			}{
				{"empty", []uint{}, -1},
				{"nil", nil, -1},
				{"first", []uint{0}, 0},
				{"second", []uint{1}, 1},
				{"third", []uint{2}, 2},
				{"ignore suffix one", []uint{0, 1}, 0},
				{"ignore suffix two", []uint{0, 2}, 0},
				{"ignore suffix after second", []uint{1, 0}, 1},
				{"out of range", []uint{99}, -1},
				{"maximum protocol index", []uint{math.MaxUint32}, -1},
				{"large index", []uint{uint(uint64(1) << 32)}, -1},
				{"maximum signed index", []uint{uint(math.MaxInt)}, -1},
				{"signed overflow", []uint{uint(math.MaxInt) + 1}, -1},
				{"maximum index", []uint{^uint(0)}, -1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					n := fixtureNearhead()
					var dataCache cacheRpc.Interface = cacheRpc.NotFoundImpl
					var traceCache *fixtureTraceCache
					switch source {
					case "nearhead":
						block := fixtureBlock(100, common.HexToHash("0x1"), nil)
						block.Traces = traces
						require.NoError(t, n.Put(block))
					case "data-cache":
						traceCache = &fixtureTraceCache{Interface: cacheRpc.NotFoundImpl}
						dataCache = traceCache
					}
					calls := 0
					p := &fixtureProvider{call: func(_ context.Context, out interface{}, method string, args ...interface{}) error {
						calls++
						require.Equal(t, "trace_get", method)
						require.Equal(t, txHash, args[0])
						indices := args[1].([]hexutil.Uint)
						require.Len(t, indices, len(tc.indexes))
						for i := range indices {
							require.Equal(t, hexutil.Uint(tc.indexes[i]), indices[i])
						}
						// Model the pinned node implementation, independently of TraceAddress.
						var trace *wt.LocalizedTrace
						if len(indices) > 0 && uint(indices[0]) < uint(len(traces)) {
							trace = &traces[uint(indices[0])]
						}
						raw, err := json.Marshal(trace)
						if err != nil {
							return err
						}
						return json.Unmarshal(raw, out)
					}}
					c := traceCacheClient{RpcTraceClient: client.NewRpcTraceClient(p), cacheClientContext: cacheClientContext{nearhead: n, dataCache: dataCache, resolver: fixtureResolver{}}}
					lazy, err := c.LazyTrace(txHash, tc.indexes)
					require.NoError(t, err)
					got, err := lazy.Load()
					require.NoError(t, err)
					if tc.want < 0 {
						require.Nil(t, got)
					} else {
						require.NotNil(t, got)
						require.Equal(t, traces[tc.want].TraceAddress, got.TraceAddress)
					}
					wantCalls := 0
					if len(tc.indexes) > 0 && (source == "fullnode" || tc.want < 0) {
						wantCalls = 1
					}
					require.Equal(t, wantCalls, calls)
					if traceCache != nil {
						require.Zero(t, traceCache.bulkCalls, "trace_get must not fetch all transaction traces")
						if len(tc.indexes) > 0 && uint64(tc.indexes[0]) <= math.MaxUint32 {
							require.Equal(t, []uint{tc.indexes[0]}, traceCache.indexes)
						} else {
							require.Empty(t, traceCache.indexes, "unsafe index must bypass data-cache")
						}
					}
				})
			}
		})
	}
}
