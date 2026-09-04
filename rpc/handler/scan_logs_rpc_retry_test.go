package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanLogsCheckpointClientRetrySuccessAndExhaustion(t *testing.T) {
	tests := []struct {
		name        string
		failures    int32
		wantSuccess bool
		wantCalls   int32
	}{
		{name: "one failure then success", failures: 1, wantSuccess: true, wantCalls: 2},
		{name: "retry budget exhausted", failures: 3, wantCalls: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				call := calls.Add(1)
				if call <= test.failures {
					_, _ = fmt.Fprint(w, `{`)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, err := fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"number":"0x1","hash":"0x0000000000000000000000000000000000000000000000000000000000000001"}}`)
				require.NoError(t, err)
			}))
			defer server.Close()

			client, err := rpcutil.NewEthClient(
				server.URL,
				rpcutil.WithClientRetryCount(2),
				rpcutil.WithClientRetryInterval(time.Millisecond),
				rpcutil.WithClientRequestTimeout(10*time.Second),
				rpcutil.WithClientHookMetrics(false),
			)
			require.NoError(t, err)

			var result map[string]string
			err = client.CallContext(
				context.Background(), &result, "eth_getBlockByNumber", "0x1", false,
			)
			if test.wantSuccess {
				require.NoError(t, err)
				assert.Equal(t, "0x1", result["number"])
			} else {
				require.Error(t, err)
			}
			assert.Equal(t, test.wantCalls, calls.Load())
		})
	}
}
