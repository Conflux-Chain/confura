package node

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Conflux-Chain/confura/node/router/proto"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	providerrpc "github.com/openweb3/go-rpc-provider"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestManagerEmptyRouteSkipsLocal(t *testing.T) {
	setupNodeTest(t)
	local := NewLocalRouter(map[Group][]string{GroupEthHttp: {"http://127.0.0.1:19001"}})
	chain := NewChainedRouter(map[Group]UrlConfig{GroupEthHttp: {Failover: "http://127.0.0.1:19009"}}, routerFunc(func(context.Context, Group, []byte) (string, error) { return "", ErrClientUnavailable }), local)
	url, err := chain.Route(context.Background(), GroupEthHttp, nil)
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:19009", url)
}

func TestUnavailableRoutePreservesErrorIdentity(t *testing.T) {
	setupNodeTest(t)
	emptyManager := routerFunc(func(context.Context, Group, []byte) (string, error) {
		return "", ErrClientUnavailable
	})
	for _, chain := range []Router{
		NewChainedRouter(nil),
		NewChainedRouter(nil, NewLocalRouter(nil)),
		NewChainedRouter(nil, emptyManager),
	} {
		_, err := chain.Route(context.Background(), GroupCfxArchives, nil)
		require.True(t, err == ErrClientUnavailable, "pure no-node responses must retain sentinel identity")
	}
	outage := errors.New("fixture transport failure")
	sourceErr := &net.OpError{Op: "read", Net: "tcp", Err: outage}
	chain := NewChainedRouter(nil, routerFunc(func(context.Context, Group, []byte) (string, error) {
		return "", sourceErr
	}))
	_, err := chain.Route(context.Background(), GroupCfxArchives, nil)
	require.ErrorIs(t, err, ErrClientUnavailable)
	require.ErrorIs(t, err, sourceErr)
}

func nodeRPCFixture(t *testing.T, h *apiHandler, unavailable *atomic.Bool) (*httptest.Server, *gethrpc.Client) {
	t.Helper()
	server := providerrpc.NewServer()
	require.NoError(t, server.RegisterName("node", &api{h: h}))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if unavailable != nil && unavailable.Load() {
			http.Error(w, "fixture unavailable", http.StatusServiceUnavailable)
			return
		}
		server.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { srv.Close(); server.Stop() })
	client, err := gethrpc.DialHTTP(srv.URL)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return srv, client
}

type switchableGRPCRouter struct {
	*grpcRouterServer
	unavailable *atomic.Bool
}

func (s switchableGRPCRouter) Route(ctx context.Context, req *pb.RouteRequest) (*pb.RouteResponse, error) {
	if s.unavailable.Load() {
		return nil, status.Error(codes.Unavailable, "fixture unavailable")
	}
	return s.grpcRouterServer.Route(ctx, req)
}
func grpcManagerFixture(t *testing.T, h *apiHandler, unavailable *atomic.Bool) Router {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterRouterServer(server, switchableGRPCRouter{&grpcRouterServer{handler: h}, unavailable})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.DialContext(context.Background(), "passthrough:///fixture", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return &NodeGRPCRouter{client: pb.NewRouterClient(conn), timeout: time.Second}
}

func TestManagerRoutingAvailabilityMatrix(t *testing.T) {
	for _, protocol := range []string{"jsonrpc", "grpc"} {
		t.Run(protocol, func(t *testing.T) {
			setupNodeTest(t)
			const managed = "http://127.0.0.1:19001"
			const failover = "http://127.0.0.1:19009"
			h := &apiHandler{pool: newNodePool(testNodeFactory)}
			require.NoError(t, h.pool.add(GroupEthHttp, managed))
			defer h.pool.del(GroupEthHttp, managed)
			var outage atomic.Bool
			_, client := nodeRPCFixture(t, h, &outage)
			var remote Router = NewNodeRpcRouter(client)
			if protocol == "grpc" {
				remote = grpcManagerFixture(t, h, &outage)
			}
			local := NewLocalRouter(nil)
			require.NoError(t, local.pollOnce(client))
			var lowerCalls atomic.Int32
			lower := routerFunc(func(context.Context, Group, []byte) (string, error) { lowerCalls.Add(1); return "", nil })
			conf := map[Group]UrlConfig{GroupEthHttp: {Failover: failover}}
			chain := NewChainedRouter(conf, remote, lower, local)
			route := func(want string) {
				t.Helper()
				url, err := chain.Route(context.Background(), GroupEthHttp, nil)
				require.NoError(t, err)
				require.Equal(t, want, url)
			}
			route(managed)
			manager, _ := h.pool.manager(GroupEthHttp)
			manager.ReportUnhealthy(rpcutil.Url2NodeName(managed), false, errors.New("offline"))
			route(failover)
			require.Zero(t, lowerCalls.Load(), "authoritative empty must stop lower-priority sources")
			_, err := NewChainedRouter(nil, remote, local).Route(context.Background(), GroupEthHttp, nil)
			require.ErrorIs(t, err, ErrClientUnavailable)
			outage.Store(true)
			require.Error(t, local.pollOnce(client))
			route(managed)
			outage.Store(false)
			route(failover)
			manager.ReportHealthy(rpcutil.Url2NodeName(managed))
			route(managed)
			h.pool.del(GroupEthHttp, managed)
			require.NoError(t, local.pollOnce(client))
			require.Empty(t, local.ListNodesByGroup(GroupEthHttp))
			route(failover) // Unknown group has the same meaning in both protocols.
			outage.Store(true)
			route(failover) // Deleted members must not reappear during an outage.
		})
	}
}

func TestRoutingRejectionsDoNotUseFallback(t *testing.T) {
	setupNodeTest(t)
	for _, failure := range []error{
		errors.New("invalid response"),
		&json.SyntaxError{},
		gethrpc.HTTPError{StatusCode: 401},
		gethrpc.HTTPError{StatusCode: 403},
		status.Error(codes.PermissionDenied, "fixture denied"),
		status.Error(codes.InvalidArgument, "invalid request"),
		&url.Error{Op: "Post", URL: "https://fixture", Err: x509.UnknownAuthorityError{}},
	} {
		var calls atomic.Int32
		chain := NewChainedRouter(map[Group]UrlConfig{GroupEthHttp: {Failover: "failover"}}, routerFunc(func(context.Context, Group, []byte) (string, error) { return "", failure }), routerFunc(func(context.Context, Group, []byte) (string, error) { calls.Add(1); return "local", nil }))
		_, err := chain.Route(context.Background(), GroupEthHttp, nil)
		require.Equal(t, failure, err)
		require.Zero(t, calls.Load())
	}
}

func TestStartupOutageUsesPresetsUntilSuccessfulSnapshot(t *testing.T) {
	setupNodeTest(t)
	var outage atomic.Bool
	outage.Store(true)
	h := &apiHandler{pool: newNodePool(testNodeFactory)}
	srv, client := nodeRPCFixture(t, h, &outage)
	const preset = "http://127.0.0.1:19001"
	const failover = "http://127.0.0.1:19009"
	chain := MustNewRouter("", srv.URL, "", map[Group]UrlConfig{GroupEthHttp: {Nodes: []string{preset}, Failover: failover}})
	remote := chain.(*chainedRouter).routers[0].(*NodeRpcRouter)
	defer remote.client.Close()
	local := locateNodeProvider(chain).(*LocalRouter)
	selected, err := chain.Route(context.Background(), GroupEthHttp, nil)
	require.NoError(t, err)
	require.Equal(t, preset, selected)
	outage.Store(false)
	require.NoError(t, local.pollOnce(client)) // First authoritative snapshot is empty.
	require.Empty(t, local.ListNodesByGroup(GroupEthHttp))
	outage.Store(true)
	selected, err = chain.Route(context.Background(), GroupEthHttp, nil)
	require.NoError(t, err)
	require.Equal(t, failover, selected)
}

func fakeEthFullnode(t *testing.T, chainID string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		result := "0x64"
		if req.Method == "eth_chainId" {
			result = chainID
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestManagerShutdownKeepsFullnodeServing(t *testing.T) {
	setupNodeTest(t)
	fullnode := fakeEthFullnode(t, "0x1")
	h := &apiHandler{pool: newNodePool(testNodeFactory)}
	require.NoError(t, h.pool.add(GroupEthHttp, fullnode.URL))
	defer h.pool.del(GroupEthHttp, fullnode.URL)
	srv, client := nodeRPCFixture(t, h, nil)
	local := NewLocalRouter(nil)
	require.NoError(t, local.pollOnce(client))
	srv.Close()
	require.Error(t, local.pollOnce(client))
	provider := NewEthClientProvider(nil, nil, NewChainedRouter(nil, NewNodeRpcRouter(client), local))
	selected, err := provider.GetClientContext(context.Background(), "fixture-user")
	require.NoError(t, err)
	defer selected.Close()
	var height string
	require.NoError(t, selected.Client.CallContext(context.Background(), &height, "eth_blockNumber"))
	require.Equal(t, "0x64", height)
}

func TestOutageFallbackKeepsGroupsAndNetworksSeparate(t *testing.T) {
	setupNodeTest(t)
	a, b := fakeEthFullnode(t, "0x1"), fakeEthFullnode(t, "0x2")
	const groupA Group = "ethprivate-a"
	const groupB Group = "ethprivate-b"
	local := NewLocalRouter(map[Group][]string{groupA: {a.URL}, groupB: {b.URL}})
	outage := routerFunc(func(context.Context, Group, []byte) (string, error) {
		return "", status.Error(codes.Unavailable, "fixture outage")
	})
	provider := NewEthClientProvider(nil, nil, NewChainedRouter(nil, outage, local))
	for _, fixture := range []struct {
		group Group
		id    string
	}{{groupA, "0x1"}, {groupB, "0x2"}} {
		selected, err := provider.GetClientContext(context.Background(), "fixture-user", fixture.group)
		require.NoError(t, err)
		defer selected.Close()
		var id string
		require.NoError(t, selected.Client.CallContext(context.Background(), &id, "eth_chainId"))
		require.Equal(t, fixture.id, id)
	}
	_, err := provider.GetClientContext(context.Background(), "fixture-user", GroupEthHttp)
	require.ErrorIs(t, err, ErrClientUnavailable)
}
