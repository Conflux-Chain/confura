package cfxbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http/httptest"
	"strings"
	"testing"

	ct "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	rp "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/go-rpc-provider/interfaces"
	"github.com/openweb3/web3go"
	"github.com/stretchr/testify/require"
)

type bridgeTestProvider struct {
	interfaces.Provider
	calls  int
	method string
	args   []interface{}
	result string
	err    error
}

func (p *bridgeTestProvider) CallContext(_ context.Context, result interface{}, method string, args ...interface{}) error {
	p.calls++
	p.method = method
	p.args = args
	if p.err != nil {
		return p.err
	}
	return json.Unmarshal([]byte(p.result), result)
}
func bridgeRequest(t *testing.T, api interface{}, method, params string) map[string]json.RawMessage {
	t.Helper()
	server := rp.NewServer()
	defer server.Stop()
	require.NoError(t, server.RegisterName("cfx", api))
	req := httptest.NewRequest("POST", "http://fixture.invalid", strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, method, params)))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, req)
	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	return body
}

func TestBridgeNumericBlocksDoNotBecomeTags(t *testing.T) {
	for _, input := range []string{"0x0", "0x7fffffffffffffff", "0x8000000000000000", "0xffffffffffffffff", "0x10000000000000001"} {
		t.Run(input, func(t *testing.T) {
			p := &bridgeTestProvider{result: `"0x2a"`}
			api := NewCfxAPI(web3go.NewClientWithProvider(p), 1030, nil)
			response := bridgeRequest(t, api, "cfx_getBalance", fmt.Sprintf(`["0x1111111111111111111111111111111111111111",%q]`, input))
			number, err := hexutil.DecodeBig(input)
			require.NoError(t, err)
			var decoded EthBlockNumberOrHash
			decodeErr := json.Unmarshal([]byte(fmt.Sprintf(`%q`, input)), &decoded)
			_, constructorErr := NewEthBlockNumberOrHash(*ct.NewEpochOrBlockHashWithEpoch(ct.NewEpochNumber((*hexutil.Big)(number))))
			if number.IsInt64() {
				require.Nil(t, response["error"])
				require.Equal(t, `"0x2a"`, string(response["result"]))
				require.Equal(t, 1, p.calls)
				args, err := json.Marshal(p.args)
				require.NoError(t, err)
				require.Contains(t, string(args), `"`+input+`"`)
				require.NoError(t, decodeErr)
				require.NoError(t, constructorErr)
			} else {
				require.NotNil(t, response["error"])
				require.Zero(t, p.calls)
				require.Error(t, decodeErr)
				require.Error(t, constructorErr)
			}
		})
	}
	// The uint64 API argument must also be checked before forwarding.
	api := &CfxAPI{}
	_, err := api.GetBlockByBlockNumber(context.Background(), hexutil.Uint64(^uint64(0)), false)
	require.Error(t, err)
	_, err = numericBlockNumber(big.NewInt(-1))
	require.Error(t, err)
	for _, canonical := range []bool{false, true} {
		hash := common.HexToHash("0x1234")
		value, err := NewEthBlockNumberOrHash(*ct.NewEpochOrBlockHashWithBlockHash(ct.Hash(hash.Hex()), canonical))
		require.NoError(t, err)
		require.Equal(t, hash, *value.ToArg().BlockHash)
		require.Equal(t, canonical, value.ToArg().RequireCanonical)
	}
}

func TestBridgeRejectsLossyHexIdentities(t *testing.T) {
	for _, address := range []string{"0x", "0x1", "0xzz", "0x" + strings.Repeat("1", 39), "0x" + strings.Repeat("1", 41), "0x" + strings.Repeat("g", 40)} {
		t.Run("address/"+address, func(t *testing.T) {
			p := &bridgeTestProvider{result: `"0x2a"`}
			api := NewCfxAPI(web3go.NewClientWithProvider(p), 1030, nil)
			response := bridgeRequest(t, api, "cfx_getBalance", fmt.Sprintf(`[%q,"0x1"]`, address))
			require.NotNil(t, response["error"])
			require.Zero(t, p.calls)
		})
	}
	for _, topic := range []string{"", "0x", "0x1", "0xzz", "0x" + strings.Repeat("1", 63), "0x" + strings.Repeat("1", 65), "0x" + strings.Repeat("g", 64)} {
		for _, topics := range []string{fmt.Sprintf(`[%q]`, topic), fmt.Sprintf(`[[%q]]`, topic)} {
			t.Run("topic/"+topics, func(t *testing.T) {
				p := &bridgeTestProvider{result: `[]`}
				api := NewCfxAPI(web3go.NewClientWithProvider(p), 1030, nil)
				response := bridgeRequest(t, api, "cfx_getLogs", `[{"topics":`+topics+`}]`)
				require.NotNil(t, response["error"])
				require.Zero(t, p.calls)
			})
		}
	}
}

func TestBridgePreservesExactHexAndBase32(t *testing.T) {
	address := "0x1111111111111111111111111111111111111111"
	topic := "0x" + strings.Repeat("ab", 32)
	base32 := cfxaddress.MustNewFromHex(address, 1030).String()
	for _, input := range []string{address, base32, "0x" + strings.Repeat("0", 40)} {
		p := &bridgeTestProvider{result: `"0x2a"`}
		api := NewCfxAPI(web3go.NewClientWithProvider(p), 1030, nil)
		response := bridgeRequest(t, api, "cfx_getBalance", fmt.Sprintf(`[%q,"0x1"]`, input))
		require.Nil(t, response["error"])
		require.Equal(t, 1, p.calls)
		expected := address
		if strings.Contains(input, strings.Repeat("0", 40)) {
			expected = input
		}
		args, err := json.Marshal(p.args)
		require.NoError(t, err)
		require.Contains(t, string(args), expected)
	}
	for _, topics := range []string{fmt.Sprintf(`[%q]`, topic), fmt.Sprintf(`[[%q]]`, topic), `[null]`, `[[]]`} {
		p := &bridgeTestProvider{result: `[]`}
		api := NewCfxAPI(web3go.NewClientWithProvider(p), 1030, nil)
		response := bridgeRequest(t, api, "cfx_getLogs", fmt.Sprintf(`[{"address":[%q],"topics":%s}]`, address, topics))
		require.Nil(t, response["error"])
		require.Equal(t, 1, p.calls)
		args, err := json.Marshal(p.args)
		require.NoError(t, err)
		require.Contains(t, string(args), address)
		if strings.Contains(topics, "ab") {
			require.Contains(t, string(args), topic)
		}
	}
}
