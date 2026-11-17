package node

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func setupNodeTest(t *testing.T) {
	t.Helper()
	old := cfg
	t.Cleanup(func() { cfg = old })
	cfg.HashRing.PartitionCount = 127
	cfg.HashRing.ReplicationFactor = 20
	cfg.HashRing.Load = 1.5
	cfg.Monitor.Interval = time.Hour
	cfg.Monitor.Unhealth.Failures = 3
	cfg.Monitor.Unhealth.EpochsFallBehind = 30
	cfg.Monitor.Unhealth.LatencyPercentile = .9
	cfg.Monitor.Unhealth.MaxLatency = 3 * time.Second
	cfg.Monitor.Recover.SuccessCounter = 3
	cfg.Monitor.Recover.RemindInterval = time.Minute
	rpcutil.MustInit()
}

type testNode struct {
	*baseNode
	closes atomic.Int32
	epoch  uint64
	err    error
}

func newTestNode(group Group, name, url string) *testNode {
	n := &testNode{baseNode: newBaseNode(name, url, func() {}), epoch: 100}
	n.atomicStatus.Store(NewStatus(group, name))
	return n
}
func (n *testNode) LatestEpochNumber() (uint64, error) { return n.epoch, n.err }
func (n *testNode) Close()                             { n.closes.Add(1); s := n.Status(); s.Close() }
func testNodeFactory(group Group, name, url string) (Node, error) {
	return newTestNode(group, name, url), nil
}

func TestPoolReleasesSharedNodeAfterLastGroup(t *testing.T) {
	setupNodeTest(t)
	p := newNodePool(testNodeFactory)
	const url = "http://127.0.0.1:19001"
	require.NoError(t, p.add(GroupEthHttp, url))
	require.NoError(t, p.add(GroupEthLogs, url))
	n := p.nodes[rpcutil.Url2NodeName(url)].Node.(*testNode)
	removed, _ := p.manager(GroupEthHttp)
	remaining, _ := p.manager(GroupEthLogs)
	p.del(GroupEthHttp, url)
	require.Zero(t, n.closes.Load())
	require.Len(t, n.monitors, 1)
	s := n.Status()
	n.err = errors.New("offline")
	for i := 0; i < 3; i++ {
		s.Update(n)
		s.Report(remaining)
	}
	require.Empty(t, remaining.Route(nil))
	n.err = nil
	for i := 0; i < 3; i++ {
		s.Update(n)
		s.Report(remaining)
	}
	require.Equal(t, url, remaining.Route(nil))
	removed.ReportHealthy(n.Name())
	removed.ReportUnhealthy(n.Name(), false, errors.New("late report"))
	removed.ReportEpoch(n.Name(), 1000)
	require.Empty(t, removed.Route(nil))
	require.Empty(t, removed.monitorStatuses)
	p.del(GroupEthLogs, url)
	p.del(GroupEthLogs, url)
	require.EqualValues(t, 1, n.closes.Load())
	require.Empty(t, p.nodes)
}

func TestSharedEthNodeKeepsProbing(t *testing.T) {
	setupNodeTest(t)
	cfg.Monitor.Interval = 5 * time.Millisecond
	var probes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		probes.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": "0x64"})
	}))
	defer srv.Close()
	p := newNodePool(func(g Group, n, u string) (Node, error) { return NewEthNode(g, n, u) })
	require.NoError(t, p.add(GroupEthHttp, srv.URL))
	require.NoError(t, p.add(GroupEthLogs, srv.URL))
	defer p.del(GroupEthLogs, srv.URL)
	require.Eventually(t, func() bool { return probes.Load() >= 2 }, time.Second, time.Millisecond)
	p.del(GroupEthHttp, srv.URL)
	before := probes.Load()
	require.Eventually(t, func() bool { return probes.Load() >= before+3 }, time.Second, time.Millisecond)
	p.del(GroupEthLogs, srv.URL)
}

func TestEthNodeCloseReleasesWebSocket(t *testing.T) {
	setupNodeTest(t)
	opened, closed := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		close(opened)
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				close(closed)
				return
			}
		}
	}))
	defer srv.Close()
	n, err := NewEthNode(GroupEthWs, "close-test", strings.Replace(srv.URL, "http://", "ws://", 1))
	require.NoError(t, err)
	defer n.Client.Close()
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("websocket did not connect")
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); n.Close() }()
	}
	wg.Wait()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not release websocket")
	}
}

// Hold a health report in progress to verify that Deregister is a callback barrier.
type blockingMonitor struct {
	HealthMonitor
	entered, release chan struct{}
	enteredOnce      sync.Once
}

func (m *blockingMonitor) HealthyEpoch() uint64 {
	m.enteredOnce.Do(func() { close(m.entered) })
	<-m.release
	return 0
}
func TestDeregisterWaitsForHealthReport(t *testing.T) {
	setupNodeTest(t)
	n := newTestNode(GroupEthHttp, "barrier", "http://127.0.0.1:19001")
	defer n.Close()
	m := NewManager(GroupEthHttp)
	m.Add(n)
	gate := &blockingMonitor{HealthMonitor: m, entered: make(chan struct{}), release: make(chan struct{})}
	n.Register(gate)
	ctx, cancel := context.WithCancel(context.Background())
	cfg.Monitor.Interval = time.Millisecond
	done := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	defer func() { release(); cancel(); <-done }()
	go func() { defer close(done); n.monitor(ctx, n) }()
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("monitor did not report")
	}
	deregistered := make(chan struct{})
	go func() { n.Deregister(gate); close(deregistered) }()
	select {
	case <-deregistered:
		t.Fatal("Deregister returned with a report in progress")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case <-deregistered:
	case <-time.After(time.Second):
		t.Fatal("Deregister blocked")
	}
	cancel()
}

type closeCountingCoreClient struct {
	sdk.ClientOperator
	closes atomic.Int32
}

func (c *closeCountingCoreClient) Close() { c.closes.Add(1) }

func TestCfxNodeCloseIsIdempotent(t *testing.T) {
	setupNodeTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	client := &closeCountingCoreClient{}
	n := &CfxNode{ClientOperator: client, baseNode: newBaseNode("core-close", "http://127.0.0.1:19001", cancel)}
	n.atomicStatus.Store(NewStatus(GroupCfxHttp, n.Name()))
	n.done = make(chan struct{})
	go n.monitor(ctx, n)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); n.Close() }()
	}
	wg.Wait()
	require.EqualValues(t, 1, client.closes.Load())
	select {
	case <-n.done:
	default:
		t.Fatal("monitor still running after Close")
	}
}
