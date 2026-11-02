package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/nearhead"
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

func TestNearHeadLogTopicsAndOwnership(t *testing.T) {
	n := fixtureNearhead()
	hash := common.HexToHash("0xa")
	require.NoError(t, n.Put(fixtureBlock(100, hash, []common.Hash{hash})))
	for _, topics := range [][][]common.Hash{{{hash}, {}}, {{hash}, {hash}}} {
		logs, err := n.GetLogsByBlockHash(hash, nearhead.FilterOpt{Topics: topics})
		require.NoError(t, err)
		require.Empty(t, logs.Logs)
	}
	logs, err := n.GetLogsByBlockHash(hash, nearhead.FilterOpt{})
	require.NoError(t, err)
	require.Len(t, logs.Logs, 1)
	logs.Logs[0].Topics[0] = common.Hash{}
	again, err := n.GetLogsByBlockHash(hash, nearhead.FilterOpt{})
	require.NoError(t, err)
	require.Equal(t, hash, again.Logs[0].Topics[0])
	n.maxLogs = 0
	_, err = n.GetLogsByBlockHash(hash, nearhead.FilterOpt{})
	require.Error(t, err)
}

func TestNearHeadHashReadsAndWriterProgress(t *testing.T) {
	n := fixtureNearhead()
	hash := common.HexToHash("0xa")
	require.NoError(t, n.Put(fixtureBlock(100, hash, nil)))
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, err := n.GetLogsByBlockHash(hash, nearhead.FilterOpt{})
				if err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 100; j++ {
			n.Pop(100)
			if err := n.Put(fixtureBlock(100, hash, nil)); err != nil {
				t.Error(err)
			}
		}
	}()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cache read/write deadlock")
	}
}
