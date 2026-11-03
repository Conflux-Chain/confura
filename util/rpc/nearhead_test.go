package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	cacheRpc "github.com/Conflux-Chain/confura-data-cache/rpc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/client"
	wt "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func TestLogsRejectReorgDuringStitch(t *testing.T) {
	for _, reorg := range []bool{false, true} {
		t.Run(fmt.Sprint(reorg), func(t *testing.T) {
			n := fixtureNearhead()
			a, b := common.HexToHash("0xa100"), common.HexToHash("0xb100")
			require.NoError(t, n.Put(fixtureBlock(100, a, nil)))
			canonical := a
			p := &fixtureProvider{call: func(_ context.Context, out interface{}, method string, args ...interface{}) error {
				var data interface{}
				switch method {
				case "eth_getBlockByNumber":
					block := fixtureBlock(100, canonical, nil).Block
					block.Transactions = *wt.NewTxOrHashListByHashes(nil)
					data = block
				case "eth_getLogs":
					if reorg {
						canonical = b
						require.Equal(t, 1, n.Pop(100))
						require.NoError(t, n.Put(fixtureBlock(100, b, nil)))
					}
					data = []wt.Log{{BlockNumber: 99, BlockHash: common.HexToHash("0x99")}}
				default:
					t.Fatalf("unexpected method %s", method)
				}
				raw, err := json.Marshal(data)
				if err != nil {
					return err
				}
				return json.Unmarshal(raw, out)
			}}
			c := ethCacheClient{RpcEthClient: client.NewRpcEthClient(p), cacheClientContext: cacheClientContext{nearhead: n, dataCache: cacheRpc.NotFoundImpl, resolver: fixtureResolver{}}}
			from, to := wt.BlockNumber(99), wt.BlockNumber(100)
			logs, err := c.Logs(wt.FilterQuery{FromBlock: &from, ToBlock: &to})
			if reorg {
				require.ErrorContains(t, err, "chain changed")
				require.Nil(t, logs)
			} else {
				require.NoError(t, err)
				require.Len(t, logs, 2)
				require.Equal(t, a, logs[1].BlockHash)
			}
		})
	}
}

// These tests cover Confura's cache/full-node boundary checks. They do not
// establish snapshot atomicity or repair data-cache's filtering and locking.
func TestLogsRangeBoundaryChecks(t *testing.T) {
	for _, tc := range []struct {
		name        string
		from, to    wt.BlockNumber
		stale       bool
		empty       bool
		nilHeader   int
		failedRPC   int
		wantHeaders int
		wantReads   int
		wantLogs    int
		wantError   string
	}{
		{name: "fully cached", from: 100, to: 100, wantHeaders: 2, wantLogs: 1},
		{name: "node suffix", from: 100, to: 101, wantHeaders: 3, wantReads: 1, wantLogs: 2},
		{name: "cache miss", from: 101, to: 101, wantReads: 1, wantLogs: 1},
		{name: "stale cache", from: 100, to: 100, stale: true, wantHeaders: 1, wantError: "differ from canonical"},
		{name: "stale empty logs", from: 100, to: 100, stale: true, empty: true, wantHeaders: 1, wantError: "differ from canonical"},
		{name: "missing anchor", from: 100, to: 100, nilHeader: 1, wantHeaders: 1, wantError: "anchor unavailable"},
		{name: "missing cache boundary", from: 100, to: 101, nilHeader: 2, wantHeaders: 2, wantError: "differ from canonical"},
		{name: "anchor RPC error", from: 100, to: 100, failedRPC: 1, wantHeaders: 1, wantError: "header RPC failed"},
		{name: "final RPC error", from: 100, to: 100, failedRPC: 2, wantHeaders: 2, wantError: "header RPC failed"},
		{name: "missing final header", from: 100, to: 100, nilHeader: 2, wantHeaders: 2, wantError: "chain changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := fixtureNearhead()
			cachedHash := common.HexToHash("0xa100")
			block := fixtureBlock(100, cachedHash, nil)
			if tc.empty {
				block.Receipts[0].Logs = nil
			}
			require.NoError(t, n.Put(block))
			headers, reads := 0, 0
			p := &fixtureProvider{call: func(_ context.Context, out interface{}, method string, args ...interface{}) error {
				var data interface{}
				switch method {
				case "eth_getBlockByNumber":
					headers++
					if headers == tc.failedRPC {
						return fmt.Errorf("header RPC failed")
					}
					if headers != tc.nilHeader {
						number := args[0].(wt.BlockNumber)
						hash := common.HexToHash("0xa101")
						if number == 100 {
							hash = cachedHash
							if tc.stale {
								hash = common.HexToHash("0xb100")
							}
						}
						header := fixtureBlock(int64(number), hash, nil).Block
						header.Transactions = *wt.NewTxOrHashListByHashes(nil)
						data = header
					}
				case "eth_getLogs":
					reads++
					data = []wt.Log{{BlockNumber: 101, BlockHash: common.HexToHash("0xa101")}}
				default:
					t.Fatalf("unexpected method %s", method)
				}
				raw, err := json.Marshal(data)
				if err != nil {
					return err
				}
				return json.Unmarshal(raw, out)
			}}
			c := ethCacheClient{RpcEthClient: client.NewRpcEthClient(p), cacheClientContext: cacheClientContext{nearhead: n, dataCache: cacheRpc.NotFoundImpl, resolver: fixtureResolver{}}}
			logs, err := c.Logs(wt.FilterQuery{FromBlock: &tc.from, ToBlock: &tc.to})
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				require.Nil(t, logs, "discard results when the boundary check fails")
			} else {
				require.NoError(t, err)
				require.Len(t, logs, tc.wantLogs)
			}
			require.Equal(t, tc.wantHeaders, headers)
			require.Equal(t, tc.wantReads, reads)
		})
	}
}
