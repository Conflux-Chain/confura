package node

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	providerrpc "github.com/openweb3/go-rpc-provider"
	"github.com/stretchr/testify/require"
)

func tlsManagerFixture(t *testing.T, version uint16) (*httptest.Server, *atomic.Bool) {
	t.Helper()
	service := providerrpc.NewServer()
	require.NoError(t, service.RegisterName("node", &api{h: &apiHandler{pool: newNodePool(testNodeFactory)}}))
	srv := httptest.NewUnstartedServer(service)
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	rejected := &atomic.Bool{}
	srv.TLS = &tls.Config{MinVersion: version, MaxVersion: version}
	srv.TLS.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		config := srv.TLS.Clone()
		if rejected.Load() {
			config.ClientAuth = tls.RequireAndVerifyClientCert
		}
		return config, nil
	}
	srv.StartTLS()
	t.Cleanup(func() { srv.Close(); service.Stop() })
	return srv, rejected
}

func tlsManagerClient(t *testing.T, srv *httptest.Server, failure string) *gethrpc.Client {
	t.Helper()
	transport := srv.Client().Transport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	switch failure {
	case "unknown authority":
		transport.TLSClientConfig.RootCAs = x509.NewCertPool()
	case "hostname":
		transport.TLSClientConfig.ServerName = "wrong-manager.invalid"
	}
	client, err := gethrpc.DialOptions(context.Background(), srv.URL, gethrpc.WithHTTPClient(&http.Client{Transport: transport}))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close(); transport.CloseIdleConnections() })
	return client
}

func TestManagerTLSFailuresUseFallback(t *testing.T) {
	for _, test := range []struct {
		name    string
		version uint16
		failure string
	}{
		{"TLS 1.2 client certificate rejected", tls.VersionTLS12, "client certificate"},
		{"TLS 1.3 client certificate rejected", tls.VersionTLS13, "client certificate"},
		{"unknown authority", tls.VersionTLS13, "unknown authority"},
		{"hostname", tls.VersionTLS13, "hostname"},
	} {
		t.Run(test.name, func(t *testing.T) {
			setupNodeTest(t)
			srv, rejected := tlsManagerFixture(t, test.version)
			rejected.Store(test.failure == "client certificate")
			remote := NewNodeRpcRouter(tlsManagerClient(t, srv, test.failure))
			_, tlsErr := remote.Route(context.Background(), GroupEthHttp, nil)
			require.Error(t, tlsErr, "the TLS failure must be observed, never bypassed")
			if test.failure != "client certificate" {
				var verification *tls.CertificateVerificationError
				require.ErrorAs(t, tlsErr, &verification)
			}
			t.Logf("actual manager TLS failure: %v", tlsErr)
			fullnode := fakeEthFullnode(t, "0x1")
			local := NewLocalRouter(map[Group][]string{GroupEthHttp: {fullnode.URL}})
			config := map[Group]UrlConfig{GroupEthHttp: {Failover: "http://127.0.0.1:19009"}}
			var lowerCalls atomic.Int32
			lower := routerFunc(func(_ context.Context, group Group, _ []byte) (string, error) {
				require.Equal(t, GroupEthHttp, group)
				lowerCalls.Add(1)
				return "http://127.0.0.1:19002", nil
			})
			url, err := NewChainedRouter(config, remote, lower, local).Route(context.Background(), GroupEthHttp, nil)
			require.NoError(t, err)
			require.Equal(t, "http://127.0.0.1:19002", url)
			require.EqualValues(t, 1, lowerCalls.Load())
			provider := NewEthClientProvider(nil, nil, NewChainedRouter(config, remote, local))
			client, err := provider.GetClientContext(context.Background(), "fixture")
			require.NoError(t, err)
			defer client.Close()
			var height string
			require.NoError(t, client.Client.CallContext(context.Background(), &height, "eth_blockNumber"))
			require.Equal(t, "0x64", height)
			url, err = NewChainedRouter(config, remote).Route(context.Background(), GroupEthHttp, nil)
			require.NoError(t, err)
			require.Equal(t, config[GroupEthHttp].Failover, url)
			_, err = NewChainedRouter(nil, remote).Route(context.Background(), GroupEthHttp, nil)
			require.ErrorIs(t, err, ErrClientUnavailable)
			if test.failure != "client certificate" {
				var verification *tls.CertificateVerificationError
				require.ErrorAs(t, err, &verification, "no-target errors must retain the TLS cause")
			}
			_, err = NewChainedRouter(nil, remote, local).Route(context.Background(), GroupEthLogs, nil)
			require.ErrorIs(t, err, ErrClientUnavailable, "fallback must not reuse another group's nodes")
		})
	}
}

func TestManagerTLSSnapshotRecovery(t *testing.T) {
	setupNodeTest(t)
	srv, rejected := tlsManagerFixture(t, tls.VersionTLS13)
	client := tlsManagerClient(t, srv, "client certificate")
	rejected.Store(true)
	const preset = "http://127.0.0.1:19001"
	local := NewLocalRouter(map[Group][]string{GroupEthHttp: {preset}})
	err := local.pollOnce(context.Background(), client)
	require.Error(t, err)
	require.True(t, isRouterUnavailable(err))
	require.Equal(t, []string{preset}, local.ListNodesByGroup(GroupEthHttp))
	rejected.Store(false)
	require.NoError(t, local.pollOnce(context.Background(), client))
	require.Empty(t, local.ListNodesByGroup(GroupEthHttp))
	rejected.Store(true)
	require.Error(t, local.pollOnce(context.Background(), client))
	require.Empty(t, local.ListNodesByGroup(GroupEthHttp))
}

func TestManagerTLSStartupUsesPresets(t *testing.T) {
	setupNodeTest(t)
	srv, _ := tlsManagerFixture(t, tls.VersionTLS13)
	const preset = "http://127.0.0.1:19001"
	const failover = "http://127.0.0.1:19009"
	// The production constructor does not trust the fixture's certificate.
	chain := MustNewRouter("", srv.URL, "", map[Group]UrlConfig{GroupEthHttp: {Nodes: []string{preset}, Failover: failover}})
	remote := chain.(*chainedRouter).routers[0].(*NodeRpcRouter)
	defer remote.client.Close()
	url, err := chain.Route(context.Background(), GroupEthHttp, nil)
	require.NoError(t, err)
	require.Equal(t, preset, url)
	local := locateNodeProvider(chain).(*LocalRouter)
	// A trusted connection gets the recovered manager's authoritative empty snapshot.
	require.NoError(t, local.pollOnce(context.Background(), tlsManagerClient(t, srv, "")))
	url, err = chain.Route(context.Background(), GroupEthHttp, nil)
	require.NoError(t, err)
	require.Equal(t, failover, url, "a later TLS failure must not revive cleared presets")
}

func TestManagerTLSHandshakeTimeoutUsesFallback(t *testing.T) {
	setupNodeTest(t)
	// Accept and drain the TLS ClientHello without completing the handshake.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan struct{})
	defer func() { listener.Close(); <-done }()
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		_, _ = io.Copy(io.Discard, conn)
	}()
	client, err := gethrpc.DialHTTP("https://" + listener.Addr().String())
	require.NoError(t, err)
	defer client.Close()
	remote := NewNodeRpcRouter(client)
	remote.timeout = 25 * time.Millisecond
	chain := NewChainedRouter(map[Group]UrlConfig{GroupEthHttp: {Failover: "fallback"}}, remote)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	url, err := chain.Route(ctx, GroupEthHttp, nil)
	require.NoError(t, err)
	require.Equal(t, "fallback", url)
	require.NoError(t, ctx.Err())
}

func TestManagerRPCProtocolErrorsRemainTerminal(t *testing.T) {
	setupNodeTest(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct{ ID json.RawMessage }
		_ = json.NewDecoder(r.Body).Decode(&request)
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID,
			"error": map[string]any{"code": -32600, "message": "fixture rejected"}})
	}))
	defer srv.Close()
	client, err := gethrpc.DialHTTP(srv.URL)
	require.NoError(t, err)
	defer client.Close()
	_, err = NewChainedRouter(map[Group]UrlConfig{GroupEthHttp: {Failover: "fallback"}}, NewNodeRpcRouter(client)).Route(context.Background(), GroupEthHttp, nil)
	var rpcError gethrpc.Error
	require.True(t, errors.As(err, &rpcError))
	require.Equal(t, -32600, rpcError.ErrorCode())
}
