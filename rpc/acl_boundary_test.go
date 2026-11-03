package rpc

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/confura/util/rpc/middlewares"
	"github.com/ethereum/go-ethereum/common"
	rpc "github.com/openweb3/go-rpc-provider"
	wt "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type ACLBoundaryAPI struct{}

func (*ACLBoundaryAPI) GetLogs(context.Context, wt.FilterQuery) ([]wt.Log, error) {
	return []wt.Log{{Address: common.HexToAddress("0x2222222222222222222222222222222222222222")}}, nil
}
func (*ACLBoundaryAPI) NewFilter(context.Context, wt.FilterQuery) (string, error) { return "0x1", nil }
func (*ACLBoundaryAPI) GetFilterLogs(context.Context, rpc.ID) ([]wt.Log, error) {
	return []wt.Log{{Address: common.HexToAddress("0x1111111111111111111111111111111111111111")}}, nil
}
func (api *ACLBoundaryAPI) GetFilterChanges(ctx context.Context, id rpc.ID) ([]wt.Log, error) {
	return api.GetFilterLogs(ctx, id)
}
func (*ACLBoundaryAPI) Echo(ctx context.Context) (string, error) {
	id, _ := handlers.GetAuthIdFromContext(ctx)
	return id, nil
}
func (*ACLBoundaryAPI) Logs(ctx context.Context, f wt.FilterQuery) (*rpc.Subscription, error) {
	n, _ := rpc.NotifierFromContext(ctx)
	return n.CreateSubscription(), nil
}

func TestContractACLHTTPBatchAndWebSocket(t *testing.T) {
	// Same production registration order; no config loading, external services, or real credentials.
	rpc.HookHandleCallMsg(middlewares.Recover)
	rpc.HookHandleCallMsg(middlewares.AntiInjection)
	rpc.HookHandleCallMsg(middlewares.Authenticate)
	rpc.HookHandleCallMsg(middlewares.Allowlists)
	rpc.HookHandleCallMsg(middlewares.DailyMaxReqRateLimit)
	rpc.HookHandleCallMsg(middlewares.QpsRateLimit)
	rpc.HookHandleCallMsg(middlewares.Metrics)
	rpc.HookHandleCallMsg(middlewares.Log)
	rpc.HookHandleCallMsg(clientMiddleware)
	rpc.HookHandleCallMsg(middlewares.UniformError)
	rpc.HookHandleCallMsg(rpc.PreventMessagesWithouID)
	const key = "FixtureProvisionedSynthetic0123456789"
	loader := rate.NewKeyLoader(func(f *rate.KeysetFilter) ([]*rate.KeyInfo, error) {
		if len(f.KeySet) == 0 || f.KeySet[0] == key {
			return []*rate.KeyInfo{{Key: key, SID: 1, AclID: 1}}, nil
		}
		return nil, nil
	})
	r := rate.NewRegistry(loader, acl.NewEthValidator)
	s := rate.NewStrategy(1, "default")
	s.LimitOptions["rpc_all_qps"] = rate.NewTokenBucketOption(10000, 10000)
	s.LimitOptions["rpc_all_daily"] = rate.FixedWindowOption{Interval: time.Hour, Quota: 10000}
	config := &rate.Config{Strategies: map[uint32]*rate.Strategy{1: s}, AllowLists: map[uint32]*acl.AllowList{1: {ID: 1, Name: "default", ContractAddresses: []string{"0x1111111111111111111111111111111111111111"}}}}
	go r.AutoReload(time.Hour, func() (*rate.Config, error) { return config, nil })
	require.Eventually(t, func() bool {
		return len(r.Strategies()) == 1 && r.Allow(acl.Context{Context: context.Background(), RpcMethod: "eth_getLogs", ExtractRpcParams: func() ([]interface{}, error) { return []interface{}{wt.FilterQuery{}}, nil }}) != nil
	}, time.Second, time.Millisecond)
	h := rpc.NewServer()
	defer h.Stop()
	require.NoError(t, h.RegisterName("eth", &ACLBoundaryAPI{}))
	httpH := httpMiddleware("eth", r, nil)(h)
	request := func(path, token, body string) string {
		q := httptest.NewRequest("POST", "http://fixture.invalid"+path, strings.NewReader(body))
		q.Header.Set("Content-Type", "application/json")
		q.Header.Set("Access-Token", token)
		w := httptest.NewRecorder()
		httpH.ServeHTTP(w, q)
		return w.Body.String()
	}
	for _, tc := range []struct{ name, path, token, body, want string }{
		{"guest", "/", "", `{"jsonrpc":"2.0","id":1,"method":"eth_echo","params":[]}`, `"result":""`},
		{"unknown", "/", "FixtureUnknownSynthetic0123456789", `{"jsonrpc":"2.0","id":1,"method":"eth_echo","params":[]}`, "invalid api key"},
		{"provisioned", "/", key, `{"jsonrpc":"2.0","id":1,"method":"eth_echo","params":[]}`, key},
		{"path beats header", "/FixtureUnknownSynthetic0123456789", key, `{"jsonrpc":"2.0","id":1,"method":"eth_echo","params":[]}`, "invalid api key"},
		{"deny getLogs", "/", "", `{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"address":"0x2222222222222222222222222222222222222222"}]}`, "invalid contract address"},
		{"malformed params", "/", "", `{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[9]}`, "bad RPC parameters"},
		{"filter denied", "/", "", `{"jsonrpc":"2.0","id":1,"method":"eth_newFilter","params":[{"address":"0x2222222222222222222222222222222222222222"}]}`, "invalid contract address"},
		{"batch", "/", "", `[{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{}]},{"jsonrpc":"2.0","id":2,"method":"eth_newFilter","params":[{}]}]`, "invalid contract address"},
		{"notification", "/", "", `{"jsonrpc":"2.0","method":"eth_echo","params":[]}`, "invalid request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := request(tc.path, tc.token, tc.body)
			require.Contains(t, out, tc.want)
			if tc.name == "batch" {
				require.NotContains(t, out, `"result"`)
			}
		})
	}
	t.Run("allowed filter remains readable", func(t *testing.T) {
		created := request("/", key, `{"jsonrpc":"2.0","id":1,"method":"eth_newFilter","params":[{"address":"0x1111111111111111111111111111111111111111"}]}`)
		var response struct {
			Result string          `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		require.NoError(t, json.Unmarshal([]byte(created), &response))
		require.Empty(t, response.Error, created)
		require.Equal(t, "0x1", response.Result)
		for _, method := range []string{"eth_getFilterLogs", "eth_getFilterChanges"} {
			out := request("/", key, `{"jsonrpc":"2.0","id":2,"method":"`+method+`","params":["`+response.Result+`"]}`)
			require.Contains(t, out, `"result":[{"address":"0x1111111111111111111111111111111111111111"`, out)
			require.NotContains(t, out, `"error"`, out)
		}
	})
	// Real WebSocket upgrade and provider subscription dispatch, with identical HTTP context injector.
	ws := httptest.NewServer(httpMiddleware("eth", r, nil)(h.WebsocketHandler([]string{"*"})))
	defer ws.Close()
	conn, err := rpc.Dial("ws" + strings.TrimPrefix(ws.URL, "http"))
	require.NoError(t, err)
	defer conn.Close()
	logs := make(chan json.RawMessage, 1)
	sub, err := conn.Subscribe(context.Background(), "eth", logs, "logs", map[string]interface{}{"address": "0x2222222222222222222222222222222222222222"})
	require.ErrorContains(t, err, "invalid contract address")
	require.Nil(t, sub)
	sub, err = conn.Subscribe(context.Background(), "eth", logs, "logs", map[string]interface{}{"address": "0x1111111111111111111111111111111111111111"})
	require.NoError(t, err)
	sub.Unsubscribe()

}
