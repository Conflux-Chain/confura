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
