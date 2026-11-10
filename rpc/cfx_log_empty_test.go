package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/store"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	ct "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	rp "github.com/openweb3/go-rpc-provider"
	"github.com/stretchr/testify/require"
)

type emptyHashTestClient struct {
	sdk.ClientOperator
	epochs, logs int
	consumed     ct.LogFilter
}

func (*emptyHashTestClient) GetNodeURL() string { return "http://fixture.invalid" }
func (c *emptyHashTestClient) GetEpochNumber(...*ct.Epoch) (*hexutil.Big, error) {
	c.epochs++
	return ct.NewBigInt(10), nil
}
func (c *emptyHashTestClient) GetLogs(f ct.LogFilter) ([]ct.Log, error) {
	c.logs++
	c.consumed = f
	return []ct.Log{}, nil
}

func TestCoreEmptyBlockHashesRemainEmpty(t *testing.T) {
	previous := store.MaxLogBlockHashesSize
	store.MaxLogBlockHashesSize = 10
	t.Cleanup(func() { store.MaxLogBlockHashesSize = previous })
	for _, configured := range []bool{false, true} {
		for _, input := range []string{`{}`, `{"blockHashes":null}`, `{"blockHashes":[]}`, `{"blockHashes":[],"fromEpoch":"0x1"}`, `{"blockHashes":[],"fromBlock":"0x1","toBlock":"0x1"}`, `{"blockHashes":["0x` + strings.Repeat("1", 64) + `"]}`} {
			t.Run(fmt.Sprintf("handler=%v/%s", configured, input), func(t *testing.T) {
				c := &emptyHashTestClient{}
				api := &cfxAPI{}
				// An empty selector must not enter the configured handler at all.
				if configured && strings.Contains(input, `"blockHashes":[]`) {
					api.LogApiHandler = &handler.CfxLogsApiHandler{}
				}
				ctx := context.WithValue(context.Background(), ctxKeyClient, c)
				server := rp.NewServer()
				defer server.Stop()
				require.NoError(t, server.RegisterName("cfx", api))
				request := httptest.NewRequest("POST", "http://fixture.invalid", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"cfx_getLogs","params":[`+input+`]}`)).WithContext(ctx)
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				server.ServeHTTP(response, request)
				var body map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
				empty := strings.Contains(input, `"blockHashes":[]`)
				conflict := empty && (strings.Contains(input, "fromEpoch") || strings.Contains(input, "fromBlock"))
				if conflict {
					require.NotNil(t, body["error"])
				} else {
					require.Nil(t, body["error"])
					require.Equal(t, `[]`, string(body["result"]))
				}
				if empty {
					require.Zero(t, c.logs)
					require.Zero(t, c.epochs)
				} else {
					require.Equal(t, 1, c.logs)
					if strings.Contains(input, strings.Repeat("1", 64)) {
						require.Len(t, c.consumed.BlockHashes, 1)
						require.Equal(t, "0x"+strings.Repeat("1", 64), c.consumed.BlockHashes[0].String())
					} else {
						require.Positive(t, c.epochs)
						require.Equal(t, "0xa", c.consumed.FromEpoch.String())
					}
				}
			})
		}
	}
}
