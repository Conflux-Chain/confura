package middlewares

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	rpc "github.com/openweb3/go-rpc-provider"
)

type MetricsFixture struct{}

func (*MetricsFixture) Ping() bool { return true }

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
