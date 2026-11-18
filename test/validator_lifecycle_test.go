package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

type validatorRPCRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
}

func waitValidator(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("validator did not stop")
	}
}

func TestValidatorTasksConcurrentStop(t *testing.T) {
	tasks := newValidatorTasks()
	var producers sync.WaitGroup
	for i := 0; i < 10; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for j := 0; j < 100; j++ {
				tasks.start(func() { tasks.waitRetry(time.Hour) })
			}
		}()
	}
	tasks.stop()
	producers.Wait()
	tasks.stop()
	var ran atomic.Bool
	tasks.start(func() { ran.Store(true) })
	require.False(t, ran.Load())
}

func TestVFValidatorCancellationUninstallsFilters(t *testing.T) {
	for _, network := range []string{"eth", "cfx"} {
		for _, nullID := range []bool{false, true} {
			t.Run(network+"/nullID="+map[bool]string{true: "true", false: "false"}[nullID], func(t *testing.T) {
				var created, polling, removed atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var req validatorRPCRequest
					if json.NewDecoder(r.Body).Decode(&req) != nil {
						return
					}
					var result interface{}
					switch req.Method {
					case "eth_newFilter", "cfx_newFilter":
						created.Add(1)
						if !nullID {
							result = "0x1"
						}
					case "eth_getFilterChanges", "cfx_getFilterChanges":
						polling.Add(1)
						<-r.Context().Done()
						return
					case "eth_uninstallFilter", "cfx_uninstallFilter":
						removed.Add(1)
						result = true
					default:
						result = "0x1"
					}
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": result})
				}))
				defer server.Close()
				conf := &VFValidConfig{server.URL, server.URL}
				var validator TestValidator
				var err error
				if network == "eth" {
					validator, err = NewEthVFValidator(conf)
				} else {
					validator, err = NewCfxVFValidator(conf)
				}
				require.NoError(t, err)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan struct{})
				go func() { var wg sync.WaitGroup; validator.Run(ctx, &wg); wg.Wait(); close(done) }()
				require.Eventually(t, func() bool {
					if nullID {
						return created.Load() >= 2
					}
					return polling.Load() == 2
				}, 4*time.Second, 10*time.Millisecond)
				cancel()
				waitValidator(t, done)
				validator.Destroy()
				validator.Destroy()
				if nullID {
					require.Zero(t, removed.Load())
				} else {
					require.Equal(t, int32(2), removed.Load())
				}
			})
		}
	}
}

func TestPubSubValidatorReconnectThenCancel(t *testing.T) {
	var subscriptions, live atomic.Int32
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		live.Add(1)
		defer live.Add(-1)
		defer conn.Close()
		for {
			var req validatorRPCRequest
			if conn.ReadJSON(&req) != nil {
				return
			}
			var result interface{} = true
			if req.Method == "eth_subscribe" {
				n := subscriptions.Add(1)
				result = "0x1"
				if n <= 2 {
					return
				} // Force reconnect while establishing the first subscriptions.
			}
			if conn.WriteJSON(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": result}) != nil {
				return
			}
		}
	}))
	defer server.Close()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	validator, err := NewEthPubSubValidator(&PSVConfig{FullnodeRpcEndpoint: url, InfuraRpcEndpoint: url})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { var wg sync.WaitGroup; validator.Run(ctx, &wg); wg.Wait(); close(done) }()
	require.Eventually(t, func() bool { return subscriptions.Load() >= 4 }, 6*time.Second, 10*time.Millisecond)
	cancel()
	waitValidator(t, done)
	validator.Destroy()
	validator.Destroy()
	require.Eventually(t, func() bool { return live.Load() == 0 }, time.Second, 10*time.Millisecond)
}
