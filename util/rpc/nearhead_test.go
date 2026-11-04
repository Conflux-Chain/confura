package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/nearhead"
	cacheRpc "github.com/Conflux-Chain/confura-data-cache/rpc"
	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/client"
	wt "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
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
					_, bounded := ctx.Deadline()
					require.False(t, bounded, "keep the original client context on every attempt")
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

// The retry window admits attempts; it does not cancel an RPC or reject a
// coherent result that completes after the window. Late ordinary errors survive.
func TestNearHeadLogsRetryBudget(t *testing.T) {
	for _, outcome := range []string{"conflict", "success", "RPC error"} {
		t.Run(outcome, func(t *testing.T) {
			logger := logrus.StandardLogger()
			output, level := logger.Out, logger.GetLevel()
			var diagnostics bytes.Buffer
			logger.SetOutput(&diagnostics)
			logger.SetLevel(logrus.InfoLevel)
			t.Cleanup(func() { logger.SetOutput(output); logger.SetLevel(level) })

			n := fixtureNearhead()
			hash := common.HexToHash("0xa100")
			require.NoError(t, n.Put(fixtureBlock(100, hash, nil)))
			var firstConflict time.Time
			headers, reads := 0, 0
			failure := fmt.Errorf("node unavailable")
			p := &fixtureProvider{call: func(ctx context.Context, out interface{}, method string, args ...interface{}) error {
				_, bounded := ctx.Deadline()
				require.False(t, bounded, "do not replace configured RPC context/timeouts")
				if method == "eth_getLogs" {
					reads++
					return nearHeadTestReply(out, []wt.Log{})
				}
				require.Equal(t, "eth_getBlockByNumber", method)
				headers++
				if headers == 1 {
					firstConflict = time.Now()
					return nearHeadTestReply(out, nil)
				}
				if headers < 4 {
					return nearHeadTestReply(out, nil)
				}
				if headers == 4 {
					require.Less(t, time.Since(firstConflict), nearHeadLogRetryTimeout, "admit the round inside the window")
					time.Sleep(time.Until(firstConflict.Add(nearHeadLogRetryTimeout + 50*time.Millisecond)))
					switch outcome {
					case "conflict":
						return nearHeadTestReply(out, nil)
					case "RPC error":
						return failure
					}
				}
				return nearHeadTestReply(out, nearHeadTestHeader(100, hash))
			}}
			from, to := wt.BlockNumber(99), wt.BlockNumber(100)
			c := nearHeadTestClient(n, p)
			c.nodeName = "https://secret@node.invalid/private-api-key"
			logs, err := c.Logs(wt.FilterQuery{FromBlock: &from, ToBlock: &to})
			require.GreaterOrEqual(t, time.Since(firstConflict), nearHeadLogRetryTimeout)
			switch outcome {
			case "conflict":
				require.Same(t, ErrInconsistentCanonicalView, err)
				require.EqualError(t, err, "inconsistent canonical view")
				require.Nil(t, logs)
				require.Equal(t, 4, headers, "no new round after expiry")
				require.Zero(t, reads)
				require.Equal(t, 1, bytes.Count(diagnostics.Bytes(), []byte("level=info")))
				require.Contains(t, diagnostics.String(), "node boundary differs from near-head cache")
				require.Contains(t, diagnostics.String(), "boundary=100")
			case "success":
				require.NoError(t, err)
				require.Len(t, logs, 1)
				require.Equal(t, 5, headers)
				require.Equal(t, 1, reads)
				require.Empty(t, diagnostics.String())
			case "RPC error":
				require.ErrorIs(t, err, failure)
				require.Nil(t, logs)
				require.Equal(t, 4, headers)
				require.Zero(t, reads)
				require.Empty(t, diagnostics.String())
			}
			require.NotContains(t, diagnostics.String(), "private-api-key")
			require.NotContains(t, diagnostics.String(), "secret")
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
// original client. Both requests must retain the configured context/deadline.
func TestNearHeadLogsRetryContextIsolation(t *testing.T) {
	n := fixtureNearhead()
	hash := common.HexToHash("0xa100")
	require.NoError(t, n.Put(fixtureBlock(100, hash, nil)))
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	headers := 0
	configuredCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := &fixtureProvider{call: func(ctx context.Context, out interface{}, method string, args ...interface{}) error {
		require.Equal(t, configuredCtx, ctx)
		if method == "eth_getLogs" {
			fq := args[0].(wt.FilterQuery)
			if *fq.FromBlock == 102 {
				require.Equal(t, configuredCtx, ctx, "shared client context was mutated")
			}
			return nearHeadTestReply(out, []wt.Log{})
		}
		headers++
		if headers == 1 {
			return nearHeadTestReply(out, nil)
		}
		if headers == 2 {
			require.Equal(t, configuredCtx, ctx, "retry context must stay unchanged")
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
	c.RpcEthClient.SetContext(configuredCtx)
	originalClient := c.RpcEthClient
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
	require.Same(t, originalClient, c.RpcEthClient)
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
				require.False(t, bounded)
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

// Wrap the real cache only at call boundaries, so mutations occur at controlled
// points without relying on scheduling or changing the dependency's readers.
type nearHeadCountingReader struct {
	*EthNearHeadCache
	t          *testing.T
	blocks     int
	scans      int
	beforeScan func()
	afterScan  func(*nearhead.EthLogs)
	missingAt  int
	scanError  error
	wantOpt    nearhead.FilterOpt
}

func (r *nearHeadCountingReader) GetBlock(ref cacheTypes.BlockHashOrNumber, full bool) nearhead.EthCacheResultWithCoverage[*wt.Block] {
	r.blocks++
	require.True(r.t, full, "metadata lookup must not build transaction hash lists")
	res := r.EthNearHeadCache.GetBlock(ref, full)
	if r.blocks == r.missingAt {
		res.Result = nil
	}
	return res
}

func (r *nearHeadCountingReader) GetLogsByBlockRange(from, to uint64, opt nearhead.FilterOpt) (*nearhead.EthLogs, error) {
	r.scans++
	require.Equal(r.t, r.wantOpt, opt)
	if r.beforeScan != nil {
		r.beforeScan()
	}
	if r.scanError != nil {
		return nil, r.scanError
	}
	res, err := r.EthNearHeadCache.GetLogsByBlockRange(from, to, opt)
	if r.afterScan != nil {
		r.afterScan(res)
	}
	return res, err
}

func TestReadNearHeadLogsSingleScan(t *testing.T) {
	a, b := common.HexToHash("0xa101"), common.HexToHash("0xb101")
	for _, tc := range []struct {
		name       string
		from, to   uint64
		wantBlocks int
		wantScans  int
		wantFrom   uint64
		wantTo     uint64
		conflict   bool
	}{
		{name: "complete", from: 100, to: 101, wantBlocks: 1, wantScans: 1, wantFrom: 100, wantTo: 101},
		{name: "empty complete", from: 100, to: 101, wantBlocks: 1, wantScans: 1, wantFrom: 100, wantTo: 101},
		{name: "miss", from: 102, to: 103, wantBlocks: 1},
		{name: "left", from: 99, to: 101, wantBlocks: 2, wantScans: 1, wantFrom: 100, wantTo: 101},
		{name: "right", from: 100, to: 102, wantBlocks: 3, wantScans: 1, wantFrom: 100, wantTo: 101},
		{name: "both", from: 99, to: 102, wantBlocks: 3, wantScans: 1, wantFrom: 100, wantTo: 101},
		{name: "disappeared", from: 99, to: 101, wantBlocks: 1, wantScans: 1, conflict: true},
		{name: "reorg before scan", from: 99, to: 101, wantBlocks: 2, wantScans: 1, conflict: true},
		{name: "reorg after scan", from: 99, to: 101, wantBlocks: 2, wantScans: 1, conflict: true},
		{name: "end expanded", from: 99, to: 102, wantBlocks: 3, wantScans: 1, conflict: true},
		{name: "expanded to complete", from: 100, to: 102, wantBlocks: 2, wantScans: 1, wantFrom: 100, wantTo: 102},
		{name: "start evicted", from: 99, to: 101, wantBlocks: 2, wantScans: 1, wantFrom: 101, wantTo: 101},
		{name: "missing before", from: 99, to: 101, wantBlocks: 1, wantScans: 1, conflict: true},
		{name: "missing after", from: 99, to: 101, wantBlocks: 2, wantScans: 1, conflict: true},
		{name: "complete missing metadata", from: 100, to: 101, wantBlocks: 1, wantScans: 1, wantFrom: 100, wantTo: 101},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := fixtureNearhead()
			first, last := fixtureBlock(100, common.HexToHash("0xa100"), nil), fixtureBlock(101, a, nil)
			if tc.name == "start evicted" {
				n.EthCache = nearhead.NewEthCache(nearhead.Config{MaxMemory: uint64(first.Size() + last.Size()), MaxLogs: 100})
			}
			if tc.name == "empty complete" {
				first.Receipts[0].Logs, last.Receipts[0].Logs = nil, nil
			}
			require.NoError(t, n.Put(first))
			require.NoError(t, n.Put(last))
			r := &nearHeadCountingReader{EthNearHeadCache: n, t: t}
			reorg := func() {
				require.Equal(t, 1, n.Pop(101))
				require.NoError(t, n.Put(fixtureBlock(101, b, nil)))
			}
			switch tc.name {
			case "disappeared":
				r.beforeScan = func() { require.Equal(t, 2, n.Pop(100)) }
			case "reorg before scan":
				r.beforeScan = reorg
			case "reorg after scan":
				r.afterScan = func(*nearhead.EthLogs) { reorg() }
			case "end expanded", "expanded to complete", "start evicted":
				r.beforeScan = func() { require.NoError(t, n.Put(fixtureBlock(102, common.HexToHash("0xa102"), nil))) }
			case "missing before", "complete missing metadata":
				r.missingAt = 1
			case "missing after":
				r.missingAt = 2
			}
			snapshot, hash, err := readNearHeadLogs(r, tc.from, tc.to, nearhead.FilterOpt{})
			require.Equal(t, tc.wantBlocks, r.blocks)
			require.Equal(t, tc.wantScans, r.scans)
			if tc.conflict {
				require.IsType(t, &nearHeadLogConflict{}, err)
				require.Nil(t, snapshot, "discard the scanned logs on conflict")
				return
			}
			require.NoError(t, err)
			if tc.wantScans == 0 {
				require.Nil(t, snapshot)
				return
			}
			require.Equal(t, tc.wantFrom, snapshot.FromBlock)
			require.Equal(t, tc.wantTo, snapshot.ToBlock)
			if tc.name == "empty complete" {
				require.Empty(t, snapshot.Logs)
			}
			if tc.wantFrom != tc.from || tc.wantTo != tc.to {
				require.Equal(t, a, hash)
			} else {
				require.Zero(t, hash, "complete cache result needs no boundary")
			}
		})
	}
}

func TestReadNearHeadLogsErrorsAndFilter(t *testing.T) {
	n := fixtureNearhead()
	topic := common.HexToHash("0x1234")
	address := common.HexToAddress("0xabcd")
	block := fixtureBlock(100, common.HexToHash("0xa100"), []common.Hash{topic})
	block.Receipts[0].Logs[0].Address = address
	require.NoError(t, n.Put(block))
	opt := nearhead.FilterOpt{Addresses: []common.Address{address}, Topics: [][]common.Hash{{topic}}}
	r := &nearHeadCountingReader{EthNearHeadCache: n, t: t, wantOpt: opt}
	snapshot, _, err := readNearHeadLogs(r, 99, 100, opt)
	require.NoError(t, err)
	require.Len(t, snapshot.Logs, 1)
	require.Equal(t, 1, r.scans)

	failure := fmt.Errorf("cache unavailable")
	r.scanError = failure
	snapshot, _, err = readNearHeadLogs(r, 99, 100, opt)
	require.ErrorIs(t, err, failure)
	require.Nil(t, snapshot)
	r.scanError = nil
	r.afterScan = func(res *nearhead.EthLogs) { res.FromBlock = 98 }
	snapshot, _, err = readNearHeadLogs(r, 99, 100, opt)
	require.EqualError(t, err, "bad event logs from near head cache: unexpected block range")
	require.Nil(t, snapshot)
	scans := r.scans
	_, _, err = readNearHeadLogs(r, 101, 100, opt)
	require.EqualError(t, err, "failed to get logs from near head cache: invalid block range params")
	require.Equal(t, scans, r.scans)
}
