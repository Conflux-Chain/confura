package middlewares

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
	rpc "github.com/openweb3/go-rpc-provider"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type MetricsFixture struct{}

func (*MetricsFixture) Ping() bool { return true }

var metricArgumentDecodes atomic.Int32

type MetricArgument string

func (a *MetricArgument) UnmarshalJSON(data []byte) error {
	metricArgumentDecodes.Add(1)
	return json.Unmarshal(data, (*string)(a))
}

func (*MetricsFixture) Required(MetricArgument) bool      { return true }
func (*MetricsFixture) Optional(*MetricArgument) bool     { return true }
func (*MetricsFixture) RawTransaction(hexutil.Bytes) bool { return true }
func (*MetricsFixture) Call(web3Types.CallRequest) bool   { return true }

func newMetricsFixtureServer(tb testing.TB) *rpc.Server {
	tb.Helper()
	server := rpc.NewServer()
	tb.Cleanup(server.Stop)
	if err := server.RegisterName("fixture", &MetricsFixture{}); err != nil {
		tb.Fatal(err)
	}
	return server
}

// Capture a real provider handler without opening a listening socket. Each
// global hook is gated by a unique context key and ignores subsequent tests.
func metricsFixtureContext(tb testing.TB) context.Context {
	tb.Helper()
	key := new(int)
	captured := make(chan context.Context, 1)
	rpc.HookHandleCallMsg(func(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
		return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
			if ctx.Value(key) != nil {
				captured <- ctx
			}
			return next(ctx, msg)
		}
	})
	server := newMetricsFixtureServer(tb)
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"fixture_ping"}`))
	req.Header.Set("Content-Type", "application/json")
	server.ServeHTTP(httptest.NewRecorder(), req.WithContext(context.WithValue(req.Context(), key, true)))
	return <-captured
}

func TestMetricMethodUsesRegisteredCallbacks(t *testing.T) {
	// The provider exposes global hooks; scope the test hook to a context key.
	type fixtureKey struct{}
	rpc.HookHandleCallMsg(func(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
		return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
			if want, ok := ctx.Value(fixtureKey{}).(string); ok {
				if got := registeredMetricMethod(ctx, msg); got != want {
					t.Errorf("%s: got %s, want %s", msg.Method, got, want)
				}
			}
			return next(ctx, msg)
		}
	})
	server := rpc.NewServer()
	defer server.Stop()
	if err := server.RegisterName("fixture", &MetricsFixture{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		method, want := fmt.Sprintf("untrusted_%d", i), "method_not_found"
		if i == 0 {
			method, want = "fixture_ping", "fixture_ping"
		}
		req := httptest.NewRequest("POST", "/", strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":[]}`, method)))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(context.WithValue(req.Context(), fixtureKey{}, want))
		server.ServeHTTP(httptest.NewRecorder(), req)
	}
}

func TestMetricMethodDoesNotDecodeParameters(t *testing.T) {
	ctx := metricsFixtureContext(t)
	for _, tc := range []struct {
		method, params, want string
	}{
		{"fixture_required", `["payload"]`, "fixture_required"},
		{"fixture_required", `[123]`, "fixture_required"},
		{"fixture_required", `not JSON`, "fixture_required"},
		{"fixture_required", `[]`, "fixture_required"},
		{"fixture_optional", `["payload"]`, "fixture_optional"},
		{"fixture_optional", `[]`, "fixture_optional"},
		{"fixture_ping", `[]`, "fixture_ping"},
		{"fixture_unknown", `["payload"]`, "method_not_found"},
		{"eth_subscribe", `["logs",{}]`, "eth_subscribe"},
		{"eth_unsubscribe", `["0x1"]`, "eth_unsubscribe"},
		{"cfx_subscribe", `["logs",{}]`, "cfx_subscribe"},
		{"cfx_unsubscribe", `["0x1"]`, "cfx_unsubscribe"},
	} {
		t.Run(tc.method+"/"+tc.params, func(t *testing.T) {
			msg := &rpc.JsonRpcMessage{Version: "2.0", ID: json.RawMessage(`1`), Method: tc.method, Params: json.RawMessage(tc.params)}
			original := *msg
			original.Params = bytes.Clone(msg.Params)
			metricArgumentDecodes.Store(0)
			require.Equal(t, tc.want, registeredMetricMethod(ctx, msg))
			require.Zero(t, metricArgumentDecodes.Load(), "classification must not invoke argument decoders")
			require.Equal(t, original, *msg, "classification must leave the message unchanged")
		})
	}
}

func TestMetricsDecodesOnlyForDispatch(t *testing.T) {
	key := new(int)
	rpc.HookHandleCallMsg(func(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
		return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
			mode, active := ctx.Value(key).(string)
			if !active {
				return next(ctx, msg)
			}
			callNext := next
			if mode == "routing failure" {
				callNext = func(_ context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
					return msg.ErrorResponse(errors.New("fixture route unavailable"))
				}
			}
			return Metrics(callNext)(ctx, msg)
		}
	})
	server := newMetricsFixtureServer(t)
	for _, mode := range []string{"success", "routing failure"} {
		t.Run(mode, func(t *testing.T) {
			metricArgumentDecodes.Store(0)
			payload := strings.Repeat("a", 128<<10)
			body := `{"jsonrpc":"2.0","id":1,"method":"fixture_required","params":["` + payload + `"]}`
			req := httptest.NewRequest("POST", "/", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			server.ServeHTTP(resp, req.WithContext(context.WithValue(req.Context(), key, mode)))
			var msg rpc.JsonRpcMessage
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &msg))
			if mode == "success" {
				require.Nil(t, msg.Error)
				require.JSONEq(t, `true`, string(msg.Result))
				require.EqualValues(t, 1, metricArgumentDecodes.Load())
			} else {
				require.EqualError(t, msg.Error, "fixture route unavailable")
				require.Zero(t, metricArgumentDecodes.Load())
			}
		})
	}
}

type metricLookupContext struct {
	context.Context
	lookups int
}

func (c *metricLookupContext) Value(key interface{}) interface{} {
	if key == "handler" {
		c.lookups++
	}
	return c.Context.Value(key)
}

func TestMetricsSkipsClassificationForNilResponse(t *testing.T) {
	ctx := &metricLookupContext{Context: context.Background()}
	next := func(context.Context, *rpc.JsonRpcMessage) *rpc.JsonRpcMessage { return nil }
	require.Nil(t, Metrics(next)(ctx, &rpc.JsonRpcMessage{Method: "fixture_required"}))
	require.Zero(t, ctx.lookups)
}

func BenchmarkRegisteredMetricMethod(b *testing.B) {
	ctx := metricsFixtureContext(b)
	for _, size := range []int{32, 128 << 10} {
		hex := "0x" + strings.Repeat("ab", size)
		for _, tc := range []struct{ name, method, params string }{
			{"raw", "fixture_rawTransaction", `["` + hex + `"]`},
			{"call", "fixture_call", `[{"data":"` + hex + `"}]`},
		} {
			b.Run(fmt.Sprintf("%s/%d", tc.name, size), func(b *testing.B) {
				msg := &rpc.JsonRpcMessage{Method: tc.method, Params: json.RawMessage(tc.params)}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if registeredMetricMethod(ctx, msg) != tc.method {
						b.Fatal("registered method misclassified")
					}
				}
			})
		}
	}
}
