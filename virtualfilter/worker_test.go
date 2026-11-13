package virtualfilter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Conflux-Chain/confura/node"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/openweb3/web3go"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	rpc "github.com/openweb3/go-rpc-provider"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type testPollingClient struct {
	chain   func() filterChain
	changes filterChanges
	err     error
}

func (c *testPollingClient) establish(context.Context) (pollingSession, error) {
	chain := filterChain(newEthFilterChain(2))
	if c.chain != nil {
		chain = c.chain()
	}
	return *newPollingSession("0xabc", chain), nil
}
func (c *testPollingClient) fetch(context.Context, rpc.ID) (filterChanges, error) {
	return c.changes, c.err
}
func (c *testPollingClient) uninstall(context.Context, rpc.ID) (bool, error) { return true, nil }

func testWorker(t *testing.T, c pollingClient, chain filterChain) *filterWorker {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() { cancel(); wg.Wait() })
	return &filterWorker{space: "eth", nodeName: "fixture", client: c, session: *newPollingSession("0xabc", chain), shutdownCtx: cmdutil.GracefulShutdownContext{Ctx: ctx, Wg: wg}}
}

func TestRejectMalformedChanges(t *testing.T) {
	cases := []filterChanges{nil, (*ethtypes.FilterChanges)(nil), (*cfxtypes.CfxFilterChanges)(nil),
		&cfxtypes.CfxFilterChanges{Logs: []*cfxtypes.SubscriptionLog{nil}},
		&cfxtypes.CfxFilterChanges{Logs: []*cfxtypes.SubscriptionLog{{}}},
		&cfxtypes.CfxFilterChanges{Logs: []*cfxtypes.SubscriptionLog{{Log: &cfxtypes.Log{}}}},
		&cfxtypes.CfxFilterChanges{Logs: []*cfxtypes.SubscriptionLog{{ChainReorg: &cfxtypes.ChainReorg{}}}},
	}
	for _, changes := range cases {
		w := testWorker(t, &testPollingClient{changes: changes}, newEthFilterChain(2))
		require.ErrorIs(t, w.pollOnce(), errInvalidFilterChanges)
		require.Nil(t, w.session.fchain)
		require.NotNil(t, w.closing)
	}
	for _, changes := range []filterChanges{&ethtypes.FilterChanges{}, &cfxtypes.CfxFilterChanges{Type: "empty"}} {
		w := testWorker(t, &testPollingClient{changes: changes}, newEthFilterChain(2))
		require.NoError(t, validateLogChanges(changes))
		if _, ok := changes.(*ethtypes.FilterChanges); ok {
			require.NoError(t, w.pollOnce())
		}
	}
}

func TestSDKNullFilterResults(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		t.Run(space, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					ID     json.RawMessage `json:"id"`
					Method string          `json:"method"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				var result interface{}
				if req.Method == "cfx_getStatus" {
					result = map[string]string{"networkId": "0x1", "chainId": "0x1"}
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": result}))
			}))
			defer srv.Close()
			var client pollingClient
			if space == "eth" {
				c, err := web3go.NewClient(srv.URL)
				require.NoError(t, err)
				client = &ethFilterWorker{client: &node.Web3goClient{Client: c, URL: srv.URL}}
			} else {
				c, err := sdk.NewClient(srv.URL)
				require.NoError(t, err)
				client = &cfxFilterWorker{client: c}
			}
			_, err := client.establish(context.Background())
			require.ErrorIs(t, err, errInvalidFilterChanges)
			w := testWorker(t, client, newEthFilterChain(2))
			require.ErrorIs(t, w.pollOnce(), errInvalidFilterChanges)
		})
	}
}

func TestUpstreamRecoveryContract(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		t.Run(space, func(t *testing.T) {
			_, s := newTestLogSession(t, space, 1)
			c := &testPollingClient{err: errors.New("connection reset")}
			s.w.client = c
			require.NoError(t, s.w.pollOnce())
			require.Contains(t, s.w.session.fcursors, s.f.fid())
			c.err = nil
			c.changes = s.batch(1, 2)
			require.NoError(t, s.w.pollOnce())
			got, err := s.f.fetch(context.Background())
			require.NoError(t, err)
			require.Equal(t, 2, logCount(got))
			c.err = errFilterNotFound
			require.ErrorIs(t, s.w.pollOnce(), errFilterNotFound)
			_, err = s.f.fetch(context.Background())
			require.ErrorIs(t, err, errFilterNotFound)
			require.NotNil(t, s.w.closing)
		})
	}
}

func BenchmarkDelegateRead(b *testing.B) {
	for _, sameID := range []bool{true, false} {
		name := "differentIDs"
		if sameID {
			name = "sameID"
		}
		b.Run(name, func(b *testing.B) {
			w := &filterWorker{session: *newPollingSession("0xabc", newEthFilterChain(1))}
			count := runtime.GOMAXPROCS(0)
			ids := make([]rpc.ID, count)
			for i := range ids {
				ids[i] = rpc.ID(fmt.Sprintf("0x%x", i+1))
				w.session.fcursors[ids[i]] = &filterDelegate{}
			}
			var next atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				idx := int(next.Add(1) - 1)
				if sameID {
					idx = 0
				}
				id := ids[idx]
				for pb.Next() {
					_, unlock, err := w.beginRead(context.Background(), id)
					if err != nil {
						b.Fatal(err)
					}
					unlock()
				}
			})
		})
	}
}
