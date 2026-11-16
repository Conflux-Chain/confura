package virtualfilter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	rpc "github.com/openweb3/go-rpc-provider"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func TestSDKFilterResultContract(t *testing.T) {
	for _, space := range []string{"eth", "cfx"} {
		valid := filterChanges(&ethtypes.FilterChanges{Logs: []ethtypes.Log{testEthLog(0)}})
		if space == "cfx" {
			valid = coreBatch(testCfxLog(0))
		}
		encoded, err := json.Marshal(valid)
		require.NoError(t, err)
		cases := []struct {
			name, body string
			valid      bool
		}{
			{"null", "null", false}, {"empty", "[]", true}, {"null_item", "[null]", false},
			{"empty_item", "[{}]", false}, {"scalar_item", "[1]", false},
			{"wrong_envelope", `{"logs":[]}`, false},
			{"hashes", `["0x0000000000000000000000000000000000000000000000000000000000000001"]`, false},
			{"genesis", string(encoded), true},
		}
		if space == "eth" {
			for _, field := range []string{"blockHash", "blockNumber", "logIndex"} {
				var logs []map[string]interface{}
				require.NoError(t, json.Unmarshal(encoded, &logs))
				delete(logs[0], field)
				missing, err := json.Marshal(logs)
				require.NoError(t, err)
				cases = append(cases, struct {
					name, body string
					valid      bool
				}{"missing_" + field, string(missing), false})
			}
		}
		if space == "cfx" {
			cases = append(cases, struct {
				name, body string
				valid      bool
			}{"revert_zero", `[{"revertTo":"0x0"}]`, true},
				struct {
					name, body string
					valid      bool
				}{"revert_overflow", `[{"revertTo":"0x10000000000000000"}]`, false})
		}
		for _, tc := range cases {
			t.Run(space+"/"+tc.name, func(t *testing.T) {
				var polls atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var req struct {
						ID     json.RawMessage
						Method string
					}
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
						return
					}
					result := json.RawMessage(`"0xabc"`)
					if req.Method == "cfx_getStatus" {
						result = json.RawMessage(`{"networkId":"0x1","chainId":"0x1"}`)
					}
					if strings.HasSuffix(req.Method, "getFilterChanges") {
						polls.Add(1)
						result = json.RawMessage(tc.body)
					}
					if err := json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": result}); err != nil {
						t.Error(err)
					}
				}))
				defer srv.Close()
				w := testWorker(t, nil, nil)
				var upstream pollingClient
				if space == "eth" {
					c, err := newEthFilterApi(nil).loadOrGetFnClient(srv.URL)
					require.NoError(t, err)
					defer c.Provider().Close()
					upstream = newEthFilterWorker(1, 100, nil, c, w.shutdownCtx)
				} else {
					c, err := newCfxFilterApi(nil).loadOrGetFnClient(srv.URL)
					require.NoError(t, err)
					defer c.Provider().Close()
					upstream = newCfxFilterWorker(1, 100, nil, c, w.shutdownCtx)
				}
				session, err := upstream.establish(context.Background())
				require.NoError(t, err)
				w.session, w.client = session, upstream
				w.session.fcursors[rpc.ID("consumer")] = &filterDelegate{cursor: session.fchain.snapshotLatestCursor()}
				err = w.pollOnce()
				if tc.valid {
					require.NoError(t, err)
					require.NotNil(t, w.session.fchain)
				} else {
					require.ErrorIs(t, err, errInvalidFilterChanges)
					require.Nil(t, w.session.fchain)
					require.NotNil(t, w.closing)
				}
				require.EqualValues(t, 1, polls.Load(), "one upstream poll; invalid decoding must not consume another response")
			})
		}
	}
}

// Ensure the validator accepts a well-formed Core log plus a revert in one batch.
func TestCoreMixedResultValidation(t *testing.T) {
	good := coreBatch(testCfxLog(1), coreRevert(0).Logs[0])
	require.NoError(t, validateLogChanges(good))
	both := testCfxLog(1)
	both.ChainReorg = &cfxtypes.ChainReorg{RevertTo: coreRevert(0).Logs[0].RevertTo}
	require.ErrorIs(t, validateLogChanges(coreBatch(both)), errInvalidFilterChanges)
}
