package rpc

import (
	"context"
	"encoding/json"
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
	return &EthNearHeadCache{maxLogs: 100, EthCache: nearhead.NewEthCache(nearhead.Config{MaxMemory: 1 << 20, MaxLogs: 100})}
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

type fixtureTraceCache struct{ cacheRpc.Interface }

func (c *fixtureTraceCache) GetTransactionTraces(common.Hash) (ct.Lazy[[]wt.LocalizedTrace], error) {
	var traces []wt.LocalizedTrace
	for _, path := range [][]uint{{}, {0}, {0, 1}, {0, 2}} {
		var trace wt.LocalizedTrace
		_ = json.Unmarshal([]byte(`{"type":"call","action":{"from":"0x0000000000000000000000000000000000000000","to":"0x0000000000000000000000000000000000000000","gas":"0x0","input":"0x","value":"0x0","callType":"call"},"traceAddress":[],"subtraces":0,"blockNumber":100,"blockHash":"0x0000000000000000000000000000000000000000000000000000000000000001"}`), &trace)
		trace.TraceAddress = path
		traces = append(traces, trace)
	}
	return ct.NewLazy(traces)
}
func TestTraceGetUsesEntirePathIncludingRoot(t *testing.T) {
	c := traceCacheClient{cacheClientContext: cacheClientContext{nearhead: fixtureNearhead(), dataCache: &fixtureTraceCache{cacheRpc.NotFoundImpl}}}
	for _, path := range [][]uint{{}, {0, 1}, {0, 2}} {
		lazy, err := c.LazyTrace(common.Hash{}, path)
		require.NoError(t, err)
		got, err := lazy.Load()
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, path, got.TraceAddress)
	}
}
