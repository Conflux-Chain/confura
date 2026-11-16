package virtualfilter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	rpc "github.com/openweb3/go-rpc-provider"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestExpiryCancelsUpstreamBacklog(t *testing.T) {
	// Exercise the production factory even when the shared RPC configuration
	// requests retries. These tests are intentionally not parallel.
	previousCfx, previousEth := viper.Get("cfx"), viper.Get("eth")
	viper.Set("cfx.retry", 3)
	viper.Set("eth.retry", 3)
	viper.Set("cfx.retryInterval", "2s")
	viper.Set("eth.retryInterval", "2s")
	rpcutil.MustInit()
	t.Cleanup(func() { viper.Set("cfx", previousCfx); viper.Set("eth", previousEth); rpcutil.MustInit() })
	for _, space := range []string{"eth", "cfx"} {
		for _, kind := range []string{"block", "pending"} {
			t.Run(space+"/"+kind, func(t *testing.T) {
				entered, release := make(chan struct{}, 1), make(chan struct{})
				var calls, created atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var req struct {
						ID     json.RawMessage
						Method string
					}
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
						return
					}
					var result interface{}
					switch {
					case strings.HasSuffix(req.Method, "uninstallFilter"):
						calls.Add(1)
						select {
						case entered <- struct{}{}:
						default:
						}
						select {
						case <-r.Context().Done():
						case <-release:
						}
						return
					case req.Method == "cfx_getStatus":
						result = map[string]string{"networkId": "0x1", "chainId": "0x1"}
					default:
						result = fmt.Sprintf("0x%x", created.Add(1))
					}
					if err := json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": result}); err != nil {
						t.Error(err)
					}
				}))
				defer server.Close()
				defer close(release)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				wg := &sync.WaitGroup{}
				base := &filterSystemBase{filterMgr: newFilterManager(), shutdownCtx: cmdutil.GracefulShutdownContext{Ctx: ctx, Wg: wg}}
				var create func() (rpc.ID, error)
				if space == "eth" {
					fs := &ethFilterSystem{filterSystemBase: base}
					api := newEthFilterApi(fs)
					client, err := api.loadOrGetFnClient(server.URL)
					require.NoError(t, err)
					defer client.Provider().Close()
					create = func() (rpc.ID, error) {
						if kind == "block" {
							return fs.newBlockFilter(client)
						}
						return fs.newPendingTransactionFilter(client)
					}
				} else {
					fs := &cfxFilterSystem{filterSystemBase: base}
					api := newCfxFilterApi(fs)
					client, err := api.loadOrGetFnClient(server.URL)
					require.NoError(t, err)
					defer client.Provider().Close()
					create = func() (rpc.ID, error) {
						if kind == "block" {
							return fs.newBlockFilter(client)
						}
						return fs.newPendingTransactionFilter(client)
					}
				}
				for i := 0; i < 32; i++ {
					id, err := create()
					require.NoError(t, err)
					f, ok := base.filterMgr.get(id)
					require.True(t, ok)
					switch vf := f.(type) {
					case *ethFilter:
						vf.lastPollingTime = time.Now().Add(-time.Hour)
					case *cfxFilter:
						vf.lastPollingTime = time.Now().Add(-time.Hour)
					}
				}
				wg.Add(1)
				go base.timeoutLoop(10 * time.Millisecond)
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("expiry did not reach upstream")
				}
				cancel()
				done := make(chan struct{})
				go func() { wg.Wait(); close(done) }()
				select {
				case <-done:
				case <-time.After(rpcutil.DefaultShutdownTimeout + time.Second):
					t.Fatal("shutdown exceeded one bounded upstream call")
				}
				require.EqualValues(t, 1, calls.Load(), "no remaining entry or SDK retry may issue another call")
			})
		}
	}
}

func TestUninstallFilterWireCompatibility(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		t.Run(space, func(t *testing.T) {
			server := rpc.NewServer()
			defer server.Stop()
			base := &filterSystemBase{filterMgr: newFilterManager()}
			if space == "eth" {
				require.NoError(t, server.RegisterName(space, newEthFilterApi(&ethFilterSystem{filterSystemBase: base})))
			} else {
				require.NoError(t, server.RegisterName(space, newCfxFilterApi(&cfxFilterSystem{filterSystemBase: base})))
			}
			req := httptest.NewRequest("POST", "http://fixture.invalid", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"`+space+`_uninstallFilter","params":["0x1"]}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			server.ServeHTTP(rec, req)
			require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":true}`, rec.Body.String())
		})
	}
}
