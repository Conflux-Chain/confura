package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	cacheRpc "github.com/Conflux-Chain/confura-data-cache/rpc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/client"
	wt "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func nearHeadTestReply(out, value interface{}) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func nearHeadTestHeader(number int64, hash common.Hash) *wt.Block {
	block := fixtureBlock(number, hash, nil).Block
	block.Transactions = *wt.NewTxOrHashListByHashes(nil)
	return block
}

func nearHeadTestClient(n *EthNearHeadCache, p *fixtureProvider) ethCacheClient {
	return ethCacheClient{RpcEthClient: client.NewRpcEthClient(p), cacheClientContext: cacheClientContext{nearhead: n, dataCache: cacheRpc.NotFoundImpl, resolver: fixtureResolver{}}}
}

// A complete cache hit must not depend on a live node. Mixed reads check only
// the cached end, even when the node supplies both a prefix and a suffix.
func TestNearHeadLogsCoverage(t *testing.T) {
	for _, tc := range []struct {
		name     string
		from, to wt.BlockNumber
		empty    bool
		events   []string
		blocks   []uint64
	}{
		{name: "complete", from: 100, to: 100, blocks: []uint64{100}},
		{name: "complete empty", from: 100, to: 100, empty: true},
		{name: "miss", from: 101, to: 101, events: []string{"logs:101-101"}, blocks: []uint64{101}},
		{name: "left", from: 99, to: 100, events: []string{"header:100", "logs:99-99", "header:100"}, blocks: []uint64{99, 100}},
		{name: "right", from: 100, to: 101, events: []string{"header:100", "logs:101-101", "header:100"}, blocks: []uint64{100, 101}},
		{name: "both", from: 99, to: 101, events: []string{"header:100", "logs:99-99", "logs:101-101", "header:100"}, blocks: []uint64{99, 100, 101}},
		{name: "both empty", from: 99, to: 101, empty: true, events: []string{"header:100", "logs:99-99", "logs:101-101", "header:100"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := fixtureNearhead()
			hash := common.HexToHash("0xa100")
			block := fixtureBlock(100, hash, nil)
			if tc.empty {
				block.Receipts[0].Logs = nil
			}
			require.NoError(t, n.Put(block))
			var events []string
			p := &fixtureProvider{call: func(_ context.Context, out interface{}, method string, args ...interface{}) error {
				switch method {
				case "eth_getBlockByNumber":
					number := args[0].(wt.BlockNumber)
					require.Equal(t, wt.BlockNumber(100), number, "no start, parent, or request-upper-bound lookup")
					events = append(events, fmt.Sprintf("header:%d", number))
					return nearHeadTestReply(out, nearHeadTestHeader(100, hash))
				case "eth_getLogs":
					fq := args[0].(wt.FilterQuery)
					events = append(events, fmt.Sprintf("logs:%d-%d", *fq.FromBlock, *fq.ToBlock))
					logs := []wt.Log{}
					if !tc.empty {
						logs = append(logs, wt.Log{BlockNumber: uint64(*fq.FromBlock)})
					}
					return nearHeadTestReply(out, logs)
				default:
					t.Fatalf("unexpected RPC %s", method)
					return nil
				}
			}}
			logs, err := nearHeadTestClient(n, p).Logs(wt.FilterQuery{FromBlock: &tc.from, ToBlock: &tc.to})
			require.NoError(t, err)
			require.NotNil(t, logs)
			var blocks []uint64
			for _, entry := range logs {
				blocks = append(blocks, entry.BlockNumber)
			}
			require.Equal(t, tc.blocks, blocks)
			require.Equal(t, tc.events, events)
		})
	}
}

// Reorg either node segment between the two boundary checks. A fresh attempt
// must replace every old log, including a prefix already read before the reorg.
func TestNearHeadLogsRetryWholeStitch(t *testing.T) {
	for _, reorgAt := range []uint64{99, 101} {
		t.Run(fmt.Sprint(reorgAt), func(t *testing.T) {
			n := fixtureNearhead()
			a, b := common.HexToHash("0xa100"), common.HexToHash("0xb100")
			require.NoError(t, n.Put(fixtureBlock(100, a, nil)))
			canonical, switched, headers := a, false, 0
			var reads []uint64
			p := &fixtureProvider{call: func(ctx context.Context, out interface{}, method string, args ...interface{}) error {
				switch method {
				case "eth_getBlockByNumber":
					headers++
					require.Equal(t, wt.BlockNumber(100), args[0])
					if headers > 2 {
						_, bounded := ctx.Deadline()
						require.True(t, bounded, "retry RPCs share the internal budget")
					}
					return nearHeadTestReply(out, nearHeadTestHeader(100, canonical))
				case "eth_getLogs":
					fq := args[0].(wt.FilterQuery)
					number := uint64(*fq.FromBlock)
					reads = append(reads, number)
					servedHash := canonical
					if number == reorgAt && !switched {
						switched, canonical = true, b
						require.Equal(t, 1, n.Pop(100))
						require.NoError(t, n.Put(fixtureBlock(100, b, nil)))
					}
					return nearHeadTestReply(out, []wt.Log{{BlockNumber: number, BlockHash: servedHash}})
				default:
					t.Fatalf("unexpected RPC %s", method)
					return nil
				}
			}}
			from, to := wt.BlockNumber(99), wt.BlockNumber(101)
			logs, err := nearHeadTestClient(n, p).Logs(wt.FilterQuery{FromBlock: &from, ToBlock: &to})
			require.NoError(t, err)
			require.Len(t, logs, 3)
			for i, entry := range logs {
				require.Equal(t, uint64(99+i), entry.BlockNumber)
				require.Equal(t, b, entry.BlockHash, "discard the failed attempt's logs")
			}
			require.Equal(t, 4, headers)
			require.Equal(t, []uint64{99, 101, 99, 101}, reads)
		})
	}
}

func TestNearHeadLogsRetryBudget(t *testing.T) {
	for _, lateSuccess := range []bool{false, true} {
		t.Run(fmt.Sprint(lateSuccess), func(t *testing.T) {
			n := fixtureNearhead()
			hash := common.HexToHash("0xa100")
			require.NoError(t, n.Put(fixtureBlock(100, hash, nil)))
			var deadline time.Time
			headers := 0
			p := &fixtureProvider{call: func(ctx context.Context, out interface{}, method string, args ...interface{}) error {
				require.Equal(t, "eth_getBlockByNumber", method)
				headers++
				if headers == 1 {
					_, bounded := ctx.Deadline()
					require.False(t, bounded, "budget starts only on the first conflict")
					return nearHeadTestReply(out, nil)
				}
				current, bounded := ctx.Deadline()
				require.True(t, bounded)
				if deadline.IsZero() {
					deadline = current
				} else {
					require.Equal(t, deadline, current, "conflicts must not reset the deadline")
				}
				if headers < 4 {
					return nearHeadTestReply(out, nil)
				}
				<-ctx.Done()
				if lateSuccess {
					return nearHeadTestReply(out, nearHeadTestHeader(100, hash))
				}
				return ctx.Err()
			}}
			from, to := wt.BlockNumber(99), wt.BlockNumber(100)
			started := time.Now()
			logs, err := nearHeadTestClient(n, p).Logs(wt.FilterQuery{FromBlock: &from, ToBlock: &to})
			require.ErrorIs(t, err, ErrInconsistentCanonicalView)
			require.Nil(t, logs)
			require.Equal(t, 4, headers)
			require.WithinDuration(t, started.Add(time.Second), deadline, 100*time.Millisecond)
			require.GreaterOrEqual(t, time.Since(started), time.Second)
			require.Less(t, time.Since(started), 3*time.Second, "fixture RPCs obey the deadline")
		})
	}
}

func TestNearHeadLogsPreserveRPCErrors(t *testing.T) {
	failure := fmt.Errorf("node unavailable")
	for _, failAt := range []string{"before", "logs", "after"} {
		t.Run(failAt, func(t *testing.T) {
			n := fixtureNearhead()
			hash := common.HexToHash("0xa100")
			require.NoError(t, n.Put(fixtureBlock(100, hash, nil)))
			headers, reads := 0, 0
			p := &fixtureProvider{call: func(_ context.Context, out interface{}, method string, args ...interface{}) error {
				if method == "eth_getLogs" {
					reads++
					if failAt == "logs" {
						return failure
					}
					return nearHeadTestReply(out, []wt.Log{{BlockNumber: 99}})
				}
				require.Equal(t, "eth_getBlockByNumber", method)
				headers++
				if headers == 1 && failAt == "before" || headers == 2 && failAt == "after" {
					return failure
				}
				return nearHeadTestReply(out, nearHeadTestHeader(100, hash))
			}}
			from, to := wt.BlockNumber(99), wt.BlockNumber(100)
			logs, err := nearHeadTestClient(n, p).Logs(wt.FilterQuery{FromBlock: &from, ToBlock: &to})
			require.ErrorIs(t, err, failure)
			require.Nil(t, logs)
			if failAt == "before" {
				require.Equal(t, 1, headers)
				require.Zero(t, reads)
			} else {
				require.Equal(t, 1, reads)
			}
		})
	}
}

// Hold one request inside its retry RPC while a second request uses the same
// original client. The second request must not inherit the first's deadline.
func TestNearHeadLogsRetryContextIsolation(t *testing.T) {
	n := fixtureNearhead()
	hash := common.HexToHash("0xa100")
	require.NoError(t, n.Put(fixtureBlock(100, hash, nil)))
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	headers := 0
	p := &fixtureProvider{call: func(ctx context.Context, out interface{}, method string, args ...interface{}) error {
		if method == "eth_getLogs" {
			fq := args[0].(wt.FilterQuery)
			if *fq.FromBlock == 102 {
				if _, bounded := ctx.Deadline(); bounded {
					return fmt.Errorf("shared client context was mutated")
				}
			}
			return nearHeadTestReply(out, []wt.Log{})
		}
		headers++
		if headers == 1 {
			return nearHeadTestReply(out, nil)
		}
		if headers == 2 {
			if _, bounded := ctx.Deadline(); !bounded {
				return fmt.Errorf("retry has no deadline")
			}
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nearHeadTestReply(out, nearHeadTestHeader(100, hash))
	}}
	c := nearHeadTestClient(n, p)
	done := make(chan error, 1)
	go func() {
		from, to := wt.BlockNumber(99), wt.BlockNumber(100)
		_, err := c.Logs(wt.FilterQuery{FromBlock: &from, ToBlock: &to})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("retry did not reach the controlled RPC")
	}
	from, to := wt.BlockNumber(102), wt.BlockNumber(102)
	_, err := c.Logs(wt.FilterQuery{FromBlock: &from, ToBlock: &to})
	require.NoError(t, err)
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("retry did not finish after release")
	}
}

func TestNearHeadLogsRetryInvalidBoundary(t *testing.T) {
	for _, invalid := range []string{"missing", "hash", "number"} {
		t.Run(invalid, func(t *testing.T) {
			n := fixtureNearhead()
			hash := common.HexToHash("0xa100")
			block := fixtureBlock(100, hash, nil)
			block.Receipts[0].Logs = nil // Empty cached results still need alignment.
			require.NoError(t, n.Put(block))
			headers, reads := 0, 0
			p := &fixtureProvider{call: func(_ context.Context, out interface{}, method string, args ...interface{}) error {
				if method == "eth_getLogs" {
					reads++
					return nearHeadTestReply(out, []wt.Log{})
				}
				require.Equal(t, "eth_getBlockByNumber", method)
				headers++
				block := nearHeadTestHeader(100, hash)
				if headers == 1 {
					switch invalid {
					case "missing":
						block = nil
					case "hash":
						block.Hash = common.HexToHash("0xb100")
					case "number":
						block.Number.SetInt64(101)
					}
				}
				return nearHeadTestReply(out, block)
			}}
			from, to := wt.BlockNumber(99), wt.BlockNumber(100)
			logs, err := nearHeadTestClient(n, p).Logs(wt.FilterQuery{FromBlock: &from, ToBlock: &to})
			require.NoError(t, err)
			require.Empty(t, logs)
			require.NotNil(t, logs)
			require.Equal(t, 3, headers)
			require.Equal(t, 1, reads)
		})
	}
}

type nearHeadTestResolver struct {
	calls, latest int
}

func (r *nearHeadTestResolver) Resolve(number wt.BlockNumber) (uint64, error) {
	r.calls++
	if number >= 0 {
		return uint64(number), nil
	}
	r.latest++
	return uint64(99 + r.latest), nil
}

func TestNearHeadLogsRetryFreezesLatest(t *testing.T) {
	n := fixtureNearhead()
	hash := common.HexToHash("0xa100")
	require.NoError(t, n.Put(fixtureBlock(100, hash, nil)))
	headers := 0
	p := &fixtureProvider{call: func(_ context.Context, out interface{}, method string, args ...interface{}) error {
		if method == "eth_getBlockByNumber" {
			require.Equal(t, wt.BlockNumber(100), args[0])
			headers++
			if headers == 1 {
				return nearHeadTestReply(out, nil)
			}
			return nearHeadTestReply(out, nearHeadTestHeader(100, hash))
		}
		require.Equal(t, "eth_getLogs", method)
		fq := args[0].(wt.FilterQuery)
		require.Equal(t, wt.BlockNumber(99), *fq.FromBlock)
		require.Equal(t, wt.BlockNumber(99), *fq.ToBlock)
		return nearHeadTestReply(out, []wt.Log{})
	}}
	c := nearHeadTestClient(n, p)
	resolver := &nearHeadTestResolver{}
	c.resolver = resolver
	from, to := wt.BlockNumber(99), wt.LatestBlockNumber
	filter := wt.FilterQuery{FromBlock: &from, ToBlock: &to}
	_, err := c.Logs(filter)
	require.NoError(t, err)
	require.Equal(t, 2, resolver.calls)
	require.Equal(t, 1, resolver.latest)
	require.Equal(t, wt.LatestBlockNumber, *filter.ToBlock)
	require.Equal(t, wt.BlockNumber(99), *filter.FromBlock)
}

func TestNearHeadLogsRetryCoverageChange(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprint(complete), func(t *testing.T) {
			n := fixtureNearhead()
			hash := common.HexToHash("0xa100")
			require.NoError(t, n.Put(fixtureBlock(100, hash, nil)))
			headers, reads := 0, 0
			p := &fixtureProvider{call: func(ctx context.Context, out interface{}, method string, args ...interface{}) error {
				if method == "eth_getBlockByNumber" {
					headers++
					if headers != 1 {
						return fmt.Errorf("new single-source attempt must not check headers")
					}
					require.Equal(t, 1, n.Pop(100))
					if complete {
						require.NoError(t, n.Put(fixtureBlock(99, hash, nil)))
						require.NoError(t, n.Put(fixtureBlock(100, hash, nil)))
					}
					return nearHeadTestReply(out, nil)
				}
				require.Equal(t, "eth_getLogs", method)
				reads++
				fq := args[0].(wt.FilterQuery)
				require.Equal(t, wt.BlockNumber(99), *fq.FromBlock)
				require.Equal(t, wt.BlockNumber(100), *fq.ToBlock)
				_, bounded := ctx.Deadline()
				require.True(t, bounded)
				return nearHeadTestReply(out, []wt.Log{})
			}}
			from, to := wt.BlockNumber(99), wt.BlockNumber(100)
			logs, err := nearHeadTestClient(n, p).Logs(wt.FilterQuery{FromBlock: &from, ToBlock: &to})
			require.NoError(t, err)
			require.Equal(t, 1, headers)
			if complete {
				require.Len(t, logs, 2)
				require.Zero(t, reads)
			} else {
				require.Empty(t, logs)
				require.Equal(t, 1, reads)
			}
		})
	}
}

func TestNearHeadLogsUsesCachedEnd(t *testing.T) {
	n := fixtureNearhead()
	first, last := common.HexToHash("0xa100"), common.HexToHash("0xa101")
	topic := common.HexToHash("0x1234")
	address := common.HexToAddress("0xabcd")
	for i, hash := range []common.Hash{first, last} {
		block := fixtureBlock(int64(100+i), hash, []common.Hash{topic})
		block.Block.ParentHash = common.HexToHash("0xa99")
		if i > 0 {
			block.Block.ParentHash = first
		}
		block.Receipts[0].Logs[0].Address = address
		require.NoError(t, n.Put(block))
	}
	headers := 0
	p := &fixtureProvider{call: func(_ context.Context, out interface{}, method string, args ...interface{}) error {
		if method == "eth_getBlockByNumber" {
			headers++
			require.Equal(t, wt.BlockNumber(101), args[0], "use the cached end, not its start or the requested upper bound")
			return nearHeadTestReply(out, nearHeadTestHeader(101, last))
		}
		require.Equal(t, "eth_getLogs", method)
		fq := args[0].(wt.FilterQuery)
		require.Equal(t, []common.Address{address}, fq.Addresses)
		require.Equal(t, [][]common.Hash{{topic}}, fq.Topics)
		return nearHeadTestReply(out, []wt.Log{{BlockNumber: uint64(*fq.FromBlock)}})
	}}
	from, to := wt.BlockNumber(99), wt.BlockNumber(102)
	filter := wt.FilterQuery{FromBlock: &from, ToBlock: &to, Addresses: []common.Address{address}, Topics: [][]common.Hash{{topic}}}
	logs, err := nearHeadTestClient(n, p).Logs(filter)
	require.NoError(t, err)
	require.Equal(t, 2, headers)
	require.Len(t, logs, 4)
	for i, entry := range logs {
		require.Equal(t, uint64(99+i), entry.BlockNumber)
	}
	require.Equal(t, wt.BlockNumber(99), *filter.FromBlock)
	require.Equal(t, wt.BlockNumber(102), *filter.ToBlock)
}
