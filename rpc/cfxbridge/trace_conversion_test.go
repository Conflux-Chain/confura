package cfxbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	ct "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider/interfaces"
	"github.com/openweb3/web3go"
	wt "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

// Failure fixtures follow Conflux v2.4.0's serializer: FailedCall/FailedCreate
// emit error without result, and valid is always present.
// https://github.com/Conflux-Chain/conflux-rust/blob/205095d8d39d8410636c5898f9ebf666c364aa33/crates/client/src/rpc/types/eth/trace.rs#L198
func traceFixture(kind string, complete, reverted bool, tx int, path []uint) map[string]interface{} {
	action := map[string]interface{}{"from": "0x1111111111111111111111111111111111111111", "gas": "0x100", "value": "0x0"}
	result := map[string]interface{}{"gasUsed": "0x10"}
	if kind == "call" {
		action["callType"] = "call"
		action["to"] = "0x2222222222222222222222222222222222222222"
		action["input"] = "0x"
		result["output"] = "0x"
	} else {
		action["init"] = "0x00"
		result["code"] = "0x00"
		result["address"] = "0x3333333333333333333333333333333333333333"
	}
	trace := map[string]interface{}{"type": kind, "action": action, "traceAddress": path, "subtraces": 0, "transactionPosition": tx, "transactionHash": common.BigToHash(big.NewInt(int64(tx + 1))).Hex(), "blockNumber": 10, "blockHash": common.HexToHash("0xa").Hex(), "valid": !reverted}
	if complete {
		trace["result"] = result
	}
	if reverted {
		trace["error"] = "Reverted"
	}
	return trace
}

type traceFixtureProvider struct {
	interfaces.Provider
	raw []byte
}

func (p *traceFixtureProvider) CallContext(_ context.Context, result interface{}, method string, _ ...interface{}) error {
	if method == "eth_getBlockByHash" {
		raw, err := json.Marshal(&wt.Block{Number: big.NewInt(10), Difficulty: big.NewInt(0), Hash: common.HexToHash("0xa")})
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, result)
	}
	if method != "trace_transaction" && method != "trace_block" {
		return fmt.Errorf("unexpected method %s", method)
	}
	return json.Unmarshal(p.raw, result)
}
func traceAPIWithFixture(t *testing.T, fixtures ...map[string]interface{}) *TraceAPI {
	t.Helper()
	raw, err := json.Marshal(fixtures)
	require.NoError(t, err)
	return NewTraceAPI(web3go.NewClientWithProvider(&traceFixtureProvider{raw: raw}), 1030)
}
func TestBridgeTraceConvertsOfficialFailures(t *testing.T) {
	for _, kind := range []string{"call", "create"} {
		for _, message := range []string{"Reverted", "Out of gas"} {
			for _, nullResult := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/null=%v", kind, message, nullResult), func(t *testing.T) {
					failed := traceFixture(kind, false, true, 1, []uint{})
					failed["error"] = message
					if nullResult {
						failed["result"] = nil
					}
					api := traceAPIWithFixture(t, traceFixture("call", true, false, 0, []uint{}), failed)
					block, err := api.Block(context.Background(), ct.Hash(common.HexToHash("0xa").Hex()))
					require.NoError(t, err)
					require.Len(t, block.TransactionTraces, 2)
					failedTraces := block.TransactionTraces[1].Traces
					require.Len(t, failedTraces, 2)
					for _, trace := range failedTraces {
						require.False(t, trace.Valid)
					}
					outcome := ct.OUTCOME_FAIL
					if message == "Reverted" {
						outcome = ct.OUTCOME_REVERTED
					}
					if kind == "call" {
						result := failedTraces[1].Action.(ct.CallResult)
						require.Equal(t, outcome, result.Outcome)
						require.Zero(t, result.GasLeft.ToInt().Sign())
						require.Empty(t, result.ReturnData)
					} else {
						result := failedTraces[1].Action.(ct.CreateResult)
						require.Equal(t, outcome, result.Outcome)
						require.Zero(t, result.GasLeft.ToInt().Sign())
						require.Empty(t, result.ReturnData)
						require.Equal(t, ConvertAddress(common.Address{}, 1030), result.Addr)
					}
					api = traceAPIWithFixture(t, failed)
					traces, err := api.Transaction(context.Background(), ct.Hash(common.HexToHash("0x2").Hex()))
					require.NoError(t, err)
					require.Equal(t, failedTraces, traces)
				})
			}
		}
	}
}
func TestBridgeTracePreservesCompleteTrees(t *testing.T) {
	parent := traceFixture("call", true, true, 0, []uint{})
	parent["subtraces"] = 1
	child := traceFixture("call", true, false, 0, []uint{0})
	child["valid"] = false
	api := traceAPIWithFixture(t, parent, child)
	traces, err := api.Transaction(context.Background(), ct.Hash(common.HexToHash("0x1").Hex()))
	require.NoError(t, err)
	require.Len(t, traces, 4)
	require.Equal(t, []ct.TraceType{ct.TRACE_CALL, ct.TRACE_CALL, ct.TRACE_CALL_RESULT, ct.TRACE_CALL_RESULT}, []ct.TraceType{traces[0].Type, traces[1].Type, traces[2].Type, traces[3].Type})
	for _, trace := range traces {
		require.False(t, trace.Valid)
		require.Equal(t, ct.Hash(common.HexToHash("0x1").Hex()), trace.TransactionHash)
		require.Equal(t, ct.Hash(common.HexToHash("0xa").Hex()), trace.BlockHash)
	}
	require.Equal(t, ct.OUTCOME_SUCCESS, traces[2].Action.(ct.CallResult).Outcome)
	require.Equal(t, ct.OUTCOME_REVERTED, traces[3].Action.(ct.CallResult).Outcome)
	parentResult := traces[3].Action.(ct.CallResult)
	require.Equal(t, int64(240), parentResult.GasLeft.ToInt().Int64())
	api = traceAPIWithFixture(t, parent, child, traceFixture("create", true, false, 1, []uint{}))
	block, err := api.Block(context.Background(), ct.Hash(common.HexToHash("0xa").Hex()))
	require.NoError(t, err)
	require.Len(t, block.TransactionTraces, 2)
	require.Len(t, block.TransactionTraces[0].Traces, 4)
	require.Len(t, block.TransactionTraces[1].Traces, 2)
	require.EqualValues(t, 1, block.TransactionTraces[1].TransactionPosition)
	require.Equal(t, ct.TRACE_CREATE, block.TransactionTraces[1].Traces[0].Type)
	require.True(t, block.TransactionTraces[1].Traces[1].Valid)
}
func TestBridgeTraceMalformedDataReturnsError(t *testing.T) {
	for _, kind := range []string{"call", "create"} {
		for _, mutation := range []string{"nullResult", "missingGasUsed", "nullGas", "nullValue", "nullAction", "missingTransaction"} {
			t.Run(kind+"/"+mutation, func(t *testing.T) {
				fixture := traceFixture(kind, true, false, 0, []uint{})
				action := fixture["action"].(map[string]interface{})
				result := fixture["result"].(map[string]interface{})
				switch mutation {
				case "nullResult":
					fixture["result"] = nil
				case "missingGasUsed":
					delete(result, "gasUsed")
				case "nullGas":
					action["gas"] = nil
				case "nullValue":
					action["value"] = nil
				case "nullAction":
					fixture["action"] = nil
				case "missingTransaction":
					delete(fixture, "transactionHash")
				}
				api := traceAPIWithFixture(t, fixture)
				traces, err := api.Transaction(context.Background(), ct.Hash(common.HexToHash("0x1").Hex()))
				require.Error(t, err)
				require.Nil(t, traces)
				api = traceAPIWithFixture(t, traceFixture("call", true, false, 0, []uint{}), fixture)
				block, err := api.Block(context.Background(), ct.Hash(common.HexToHash("0xa").Hex()))
				require.Error(t, err)
				require.Nil(t, block)
			})
		}
	}
	hash := common.HexToHash("0x1")
	for _, trace := range []*wt.LocalizedTrace{
		{TransactionHash: &hash, Type: wt.TRACE_CALL, Action: wt.Create{}},
		{TransactionHash: &hash, Type: wt.TRACE_CALL, Action: wt.Call{}, Result: wt.CreateResult{}},
		{TransactionHash: &hash, Type: wt.TRACE_SUICIDE, Action: wt.Suicide{}},
	} {
		a, b, err := ConvertTrace(trace, 1030)
		require.Error(t, err)
		require.Nil(t, a)
		require.Nil(t, b)
	}
}

func TestBridgeTracePreservesLegacyMapping(t *testing.T) {
	for _, kind := range []string{"call", "create"} {
		t.Run(kind, func(t *testing.T) {
			fixture := traceFixture(kind, true, false, 0, []uint{})
			result := fixture["result"].(map[string]interface{})
			// These fields did not cause a panic and were not rejected before.
			delete(result, "output")
			delete(result, "code")
			api := traceAPIWithFixture(t, fixture)
			traces, err := api.Transaction(context.Background(), ct.Hash(common.HexToHash("0x1").Hex()))
			require.NoError(t, err)
			require.Len(t, traces, 2)
			if kind == "call" {
				result := traces[1].Action.(ct.CallResult)
				require.Equal(t, int64(240), result.GasLeft.ToInt().Int64())
				require.Nil(t, result.ReturnData)
			} else {
				result := traces[1].Action.(ct.CreateResult)
				require.Equal(t, int64(240), result.GasLeft.ToInt().Int64())
				require.Nil(t, result.ReturnData)
			}
		})
	}
	a, b, err := ConvertTrace(&wt.LocalizedTrace{Type: "unknown"}, 1030)
	require.NoError(t, err)
	require.Nil(t, a)
	require.Nil(t, b)
}

func TestBridgeTracePreservesTreeWithMissingFailedResult(t *testing.T) {
	parent := traceFixture("call", false, true, 0, []uint{})
	parent["subtraces"] = 1
	child := traceFixture("call", true, false, 0, []uint{0})
	child["valid"] = false
	api := traceAPIWithFixture(t, parent, child)
	traces, err := api.Transaction(context.Background(), ct.Hash(common.HexToHash("0x1").Hex()))
	require.NoError(t, err)
	require.Len(t, traces, 4)
	require.Equal(t, []ct.TraceType{ct.TRACE_CALL, ct.TRACE_CALL, ct.TRACE_CALL_RESULT, ct.TRACE_CALL_RESULT}, []ct.TraceType{traces[0].Type, traces[1].Type, traces[2].Type, traces[3].Type})
	for _, trace := range traces {
		require.False(t, trace.Valid)
	}
	require.Equal(t, ct.OUTCOME_SUCCESS, traces[2].Action.(ct.CallResult).Outcome)
	require.Equal(t, ct.OUTCOME_REVERTED, traces[3].Action.(ct.CallResult).Outcome)
	result := traces[3].Action.(ct.CallResult)
	require.Zero(t, result.GasLeft.ToInt().Sign())
}
