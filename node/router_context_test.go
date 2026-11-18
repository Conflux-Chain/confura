package node

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Conflux-Chain/confura/node/router/proto"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientRoutingHonorsCancellation(t *testing.T) {
	setupNodeTest(t)
	entered, release := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release }))
	defer srv.Close()
	defer close(release)
	client, err := gethrpc.DialHTTP(srv.URL)
	require.NoError(t, err)
	defer client.Close()
	var factories atomic.Int32
	provider := newClientProvider[string](nil, NewNodeRpcRouter(client), GroupEthHttp, func(_ Group, u string) (string, error) { factories.Add(1); return u, nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := provider.GetClientByIP(ctx); result <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("route did not start")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("routing ignored request cancellation")
	}
	require.Zero(t, factories.Load())
}

type routerFunc func(context.Context, Group, []byte) (string, error)

func (f routerFunc) Route(ctx context.Context, g Group, k []byte) (string, error) {
	return f(ctx, g, k)
}

func TestRoutingTimeoutReachesLocalFallback(t *testing.T) {
	setupNodeTest(t)
	cfg.Router.Timeout = 25 * time.Millisecond
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	client, err := gethrpc.DialHTTP(srv.URL)
	require.NoError(t, err)
	defer client.Close()
	local := NewLocalRouter(map[Group][]string{GroupEthHttp: {"http://127.0.0.1:19001"}})
	chain := NewChainedRouter(nil, NewNodeRpcRouter(client), local)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	url, err := chain.Route(ctx, GroupEthHttp, nil)
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:19001", url)
	require.NoError(t, ctx.Err(), "local fallback must run before the caller deadline")
}

func TestSnapshotTimeoutKeepsNodesAndAllowsRefresh(t *testing.T) {
	setupNodeTest(t)
	cfg.Router.Timeout = 25 * time.Millisecond
	var blocked atomic.Bool
	blocked.Store(true)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blocked.Load() {
			<-release
			return
		}
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[Group][]string{GroupEthHttp: {"http://127.0.0.1:19002"}}})
	}))
	defer srv.Close()
	var once sync.Once
	unblock := func() { once.Do(func() { blocked.Store(false); close(release) }) }
	defer unblock()
	client, err := gethrpc.DialHTTP(srv.URL)
	require.NoError(t, err)
	defer client.Close()
	local := NewLocalRouter(map[Group][]string{GroupEthHttp: {"http://127.0.0.1:19001"}})
	require.Error(t, local.pollOnce(context.Background(), client))
	require.Equal(t, []string{"http://127.0.0.1:19001"}, local.ListNodesByGroup(GroupEthHttp))
	unblock()
	require.NoError(t, local.pollOnce(context.Background(), client))
	require.Equal(t, []string{"http://127.0.0.1:19002"}, local.ListNodesByGroup(GroupEthHttp))
}

type blockingGRPCRouter struct {
	pb.UnimplementedRouterServer
	entered chan struct{}
}

func (s *blockingGRPCRouter) Route(ctx context.Context, _ *pb.RouteRequest) (*pb.RouteResponse, error) {
	close(s.entered)
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}
func TestGRPCRoutingHonorsCancellation(t *testing.T) {
	setupNodeTest(t)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	service := &blockingGRPCRouter{entered: make(chan struct{})}
	pb.RegisterRouterServer(server, service)
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.DialContext(context.Background(), "passthrough:///fixture", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	defer conn.Close()
	remote := &NodeGRPCRouter{client: pb.NewRouterClient(conn), timeout: time.Second}
	var fallback atomic.Int32
	chain := NewChainedRouter(nil, remote, routerFunc(func(context.Context, Group, []byte) (string, error) { fallback.Add(1); return "local", nil }))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := chain.Route(ctx, GroupEthHttp, nil); result <- err }()
	select {
	case <-service.entered:
	case <-time.After(time.Second):
		t.Fatal("grpc route did not start")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("grpc ignored cancellation")
	}
	require.Zero(t, fallback.Load())
}

func TestRedisRoutingHasDeadline(t *testing.T) {
	setupNodeTest(t)
	cfg.Router.Timeout = 25 * time.Millisecond
	client := redis.NewClient(&redis.Options{MaxRetries: -1, IdleCheckFrequency: -1, Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, peer := net.Pipe()
		t.Cleanup(func() { conn.Close(); peer.Close() })
		go io.Copy(io.Discard, peer)
		return conn, nil
	}})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := NewRedisRouter(client).Route(ctx, GroupEthHttp, nil)
	require.Error(t, err)
	require.NoError(t, ctx.Err(), "redis must honor the shorter source deadline")
}

func TestCanceledRouteDoesNotCreateClient(t *testing.T) {
	setupNodeTest(t)
	var calls atomic.Int32
	provider := newClientProvider[string](nil, routerFunc(func(ctx context.Context, g Group, k []byte) (string, error) { calls.Add(1); return "target", nil }), GroupEthHttp, func(Group, string) (string, error) { calls.Add(1); return "client", nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := provider.GetClientContext(ctx, "tenant", GroupEthLogs)
	require.ErrorIs(t, err, context.Canceled)
	_, err = provider.GetClientByIP(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls.Load())
}

func TestConcurrentClientSelectionAndPoolMembership(t *testing.T) {
	setupNodeTest(t)
	const target = "http://127.0.0.1:19001"
	pool := newNodePool(testNodeFactory)
	require.NoError(t, pool.add(GroupEthHttp, target))
	defer pool.del(GroupEthHttp, target)
	router := routerFunc(func(ctx context.Context, g Group, key []byte) (string, error) {
		manager, ok := pool.manager(g)
		if !ok {
			return "", ErrClientUnavailable
		}
		url := manager.Route(key)
		if url == "" {
			return "", ErrClientUnavailable
		}
		return url, nil
	})
	var factories atomic.Int32
	provider := newClientProvider[string](nil, router, GroupEthHttp, func(_ Group, u string) (string, error) { factories.Add(1); return u, nil })
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				client, err := provider.GetClientContext(context.Background(), "fixture-user")
				if err != nil {
					if !errors.Is(err, ErrClientUnavailable) {
						t.Error(err)
					}
					continue
				}
				if client != target {
					t.Errorf("wrong client target: %s", client)
				}
			}
		}()
	}
	for i := 0; i < 25; i++ {
		pool.del(GroupEthHttp, target)
		require.NoError(t, pool.add(GroupEthHttp, target))
	}
	wg.Wait()
	_, err := provider.GetClient("fixture-user")
	require.NoError(t, err)
	require.EqualValues(t, 1, factories.Load(), "concurrent selection must reuse one client for the same identity")
}

func TestClientSelectionCanFinishAfterMembershipRemoval(t *testing.T) {
	setupNodeTest(t)
	const target = "http://127.0.0.1:19001"
	pool := newNodePool(testNodeFactory)
	require.NoError(t, pool.add(GroupEthHttp, target))
	manager, _ := pool.manager(GroupEthHttp)
	entered, release := make(chan struct{}), make(chan struct{})
	provider := newClientProvider[string](nil, routerFunc(func(context.Context, Group, []byte) (string, error) { return manager.Route(nil), nil }), GroupEthHttp, func(_ Group, u string) (string, error) { close(entered); <-release; return u, nil })
	result := make(chan string, 1)
	go func() {
		client, err := provider.GetClient("fixture-user")
		if err != nil {
			result <- "error"
			return
		}
		result <- client
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("client factory did not start")
	}
	pool.del(GroupEthHttp, target)
	close(release)
	select {
	case client := <-result:
		require.Equal(t, target, client)
	case <-time.After(time.Second):
		t.Fatal("selected client did not finish")
	}
}
