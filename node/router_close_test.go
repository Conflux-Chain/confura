package node

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"
)

func TestLocalRouterCloseCancelsActiveSnapshot(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	client, err := gethrpc.DialHTTP(server.URL)
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	router := &LocalRouter{ctx: ctx, cancel: cancel, done: make(chan struct{}), pollStarted: true, timeout: 5 * time.Second}
	// Run the active request directly to avoid waiting for the one-minute ticker.
	requestDone := make(chan error, 1)
	go func() { defer close(router.done); requestDone <- router.pollOnce(router.ctx, client) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("snapshot RPC did not start")
	}
	closed := make(chan struct{})
	go func() { router.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close waited for the five-second RPC timeout instead of cancellation")
	}
	require.ErrorIs(t, <-requestDone, context.Canceled)
	router.Close()
}
