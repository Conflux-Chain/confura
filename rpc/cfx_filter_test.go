package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	vfclient "github.com/Conflux-Chain/confura/virtualfilter/client"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	ct "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider/interfaces"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

type filterTestProvider struct {
	interfaces.Provider
	method string
	args   []interface{}
	result string
	err    error
}

func (p *filterTestProvider) CallContext(_ context.Context, result interface{}, method string, args ...interface{}) error {
	if method == "cfx_epochNumber" {
		return json.Unmarshal([]byte(`"0xa"`), result)
	}
	p.method, p.args = method, args
	if p.err != nil {
		return p.err
	}
	return json.Unmarshal([]byte(p.result), result)
}

func TestCoreFiltersUseWrappedClient(t *testing.T) {
	sentinel := errors.New("fixture filter failure")
	tests := []struct {
		method, result, args string
		run                  func(*cfxAPI, context.Context) (interface{}, error)
	}{
		{"cfx_newFilter", `"0x123"`, `[{"fromEpoch":"0x1"}]`, func(a *cfxAPI, c context.Context) (interface{}, error) {
			return a.NewFilter(c, ct.LogFilter{FromEpoch: ct.NewEpochNumberUint64(1)})
		}},
		{"cfx_newBlockFilter", `"0x123"`, `[]`, func(a *cfxAPI, c context.Context) (interface{}, error) { return a.NewBlockFilter(c) }},
		{"cfx_newPendingTransactionFilter", `"0x123"`, `[]`, func(a *cfxAPI, c context.Context) (interface{}, error) { return a.NewPendingTransactionFilter(c) }},
		{"cfx_uninstallFilter", `true`, `["0x123"]`, func(a *cfxAPI, c context.Context) (interface{}, error) { return a.UninstallFilter(c, "0x123") }},
		{"cfx_getFilterChanges", `[]`, `["0x123"]`, func(a *cfxAPI, c context.Context) (interface{}, error) { return a.GetFilterChanges(c, "0x123") }},
		{"cfx_getFilterLogs", `[]`, `["0x123"]`, func(a *cfxAPI, c context.Context) (interface{}, error) { return a.GetFilterLogs(c, "0x123") }},
	}
	for _, test := range tests {
		t.Run(test.method, func(t *testing.T) {
			p := &filterTestProvider{result: test.result}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					ID     json.RawMessage   `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				body := map[string]interface{}{"jsonrpc": "2.0", "id": request.ID}
				if request.Method == "cfx_getStatus" {
					body["result"] = json.RawMessage(`{"chainId":"0x405","networkId":"0x405"}`)
				} else {
					var result json.RawMessage
					args := make([]interface{}, len(request.Params))
					for i := range args {
						args[i] = request.Params[i]
					}
					if err := p.CallContext(r.Context(), &result, request.Method, args...); err != nil {
						body["error"] = map[string]interface{}{"code": -32000, "message": err.Error()}
					} else {
						body["result"] = result
					}
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(body)
			}))
			defer server.Close()
			client, err := sdk.NewClient(server.URL)
			require.NoError(t, err)
			client.SetNetworkId(1029)
			client.SetChainId(1029)
			ctx := context.WithValue(context.Background(), ctxKeyClient, rpcutil.NewCfxCoreClient(client))
			api := &cfxAPI{}
			result, err := test.run(api, ctx)
			require.NoError(t, err)
			require.Equal(t, test.method, p.method)
			args, err := json.Marshal(p.args)
			require.NoError(t, err)
			if p.args == nil {
				args = []byte(`[]`)
			}
			require.JSONEq(t, test.args, string(args))
			data, err := json.Marshal(result)
			require.NoError(t, err)
			require.JSONEq(t, test.result, string(data))
			p.err = sentinel
			_, err = test.run(api, ctx)
			require.ErrorContains(t, err, sentinel.Error())
		})
	}
}

func TestCoreFilterStillUsesVirtualFilterService(t *testing.T) {
	received := make(chan []json.RawMessage, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if req.Method != "cfx_newBlockFilter" {
			http.Error(w, "unexpected method", 400)
			return
		}
		received <- req.Params
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": "0x456"})
	}))
	defer server.Close()
	previous := viper.Get("virtualFilters.client")
	viper.Set("virtualFilters.client", map[string]interface{}{"enabled": true, "serviceRpcUrl": server.URL})
	t.Cleanup(func() { viper.Set("virtualFilters.client", previous) })
	virtual, enabled := vfclient.MustNewCfxClientFromViper()
	require.True(t, enabled)
	client := &emptyHashTestClient{}
	ctx := context.WithValue(context.Background(), ctxKeyClient, client)
	api := &cfxAPI{}
	api.VirtualFilterClient = virtual
	id, err := api.NewBlockFilter(ctx)
	require.NoError(t, err)
	require.Equal(t, "0x456", string(*id))
	args := <-received
	require.Len(t, args, 1)
	require.JSONEq(t, `"http://fixture.invalid"`, string(args[0]))
}
