package rpc

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	provider "github.com/openweb3/go-rpc-provider"
	"github.com/stretchr/testify/require"
)

type lifecycleAPI struct {
	entered chan struct{}
	exited  chan struct{}
}

func (a *lifecycleAPI) Block(ctx context.Context) (bool, error) {
	close(a.entered)
	<-ctx.Done()
	close(a.exited)
	return false, ctx.Err()
}

func TestServerShutdownJoinsHTTPAndWebSocket(t *testing.T) {
	for _, protocol := range []Protocol{ProtocolHttp, ProtocolWS} {
		t.Run(string(protocol), func(t *testing.T) {
			api := &lifecycleAPI{make(chan struct{}), make(chan struct{})}
			srv, err := NewServer("fixture", map[string]interface{}{"test": api}, nil)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var wg sync.WaitGroup
			require.NoError(t, srv.Start(ctx, &wg, "127.0.0.1:0", protocol))
			scheme := "http://"
			if protocol == ProtocolWS {
				scheme = "ws://"
			}
			client, err := provider.Dial(scheme + srv.servers[protocol].Addr)
			require.NoError(t, err)
			defer client.Close()
			called := make(chan error, 1)
			go func() { var result bool; called <- client.Call(&result, "test_block") }()
			<-api.entered
			cancel()
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(6 * time.Second):
				t.Fatal("shutdown did not join requests")
			}
			select {
			case <-api.exited:
			default:
				t.Fatal("dependency release preceded request exit")
			}
			require.Error(t, <-called)
			srv.Close()
		})
	}
}

func TestLaterListenerFailureCanUnwind(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	srv, err := NewServer("fixture", map[string]interface{}{}, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	require.NoError(t, srv.Start(ctx, &wg, "127.0.0.1:0", ProtocolHttp))
	addr := srv.servers[ProtocolHttp].Addr
	require.Error(t, srv.Start(ctx, &wg, l.Addr().String(), ProtocolWS))
	cancel()
	wg.Wait()
	rebound, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	rebound.Close()
}
