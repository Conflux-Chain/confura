package virtualfilter

import (
	"context"
	"encoding/json"
	"github.com/Conflux-Chain/confura/node"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/openweb3/web3go"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	rpc "github.com/openweb3/go-rpc-provider"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type testPollingClient struct {
	changes filterChanges
	err     error
}

func (c *testPollingClient) establish() (pollingSession, error) {
	return *newPollingSession("0xabc", newEthFilterChain(2)), nil
}
func (c *testPollingClient) fetch(rpc.ID) (filterChanges, error) { return c.changes, c.err }
func (c *testPollingClient) uninstall(rpc.ID) (bool, error)      { return true, nil }

func testWorker(t *testing.T, c pollingClient, chain filterChain) *filterWorker {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &filterWorker{space: "eth", nodeName: "fixture", client: c, session: *newPollingSession("0xabc", chain), shutdownCtx: cmdutil.GracefulShutdownContext{Ctx: ctx, Wg: &sync.WaitGroup{}}}
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
			_, err := client.establish()
			require.ErrorIs(t, err, errInvalidFilterChanges)
			w := testWorker(t, client, newEthFilterChain(2))
			require.ErrorIs(t, w.pollOnce(), errInvalidFilterChanges)
		})
	}
}
