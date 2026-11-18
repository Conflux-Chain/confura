package rpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestWeb3goClientCloseWithoutCache(t *testing.T) {
	closed := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer close(closed)
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	client, err := NewEthClient("ws" + strings.TrimPrefix(server.URL, "http"))
	require.NoError(t, err)
	defer client.Close()
	wrapper := Web3goClient{Client: client}
	require.NotPanics(t, wrapper.Close)
	require.NotPanics(t, wrapper.Close)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("underlying WebSocket client was not closed")
	}
}

func TestNearHeadCloseJoinsOwnedTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cache := &EthNearHeadCache{stopSync: cancel}
	release := make(chan struct{})
	cache.syncDone.Add(1)
	go func() { defer cache.syncDone.Done(); <-ctx.Done(); <-release }()
	done := make(chan struct{})
	go func() { cache.Close(); close(done) }()
	<-ctx.Done()
	select {
	case <-done:
		t.Fatal("cache close returned before its task")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cache close did not join its task")
	}
	cache.Close()
}
