package cfxbridge

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/openweb3/web3go"
	"github.com/stretchr/testify/require"
)

func TestBridgeEpochReceiptsPreserveSelectors(t *testing.T) {
	previous := store.DefaultReceiptOption
	store.DefaultReceiptOption = store.EthReceiptOption{Method: store.EthReceiptMethodEthBlockReceipts}
	t.Cleanup(func() { store.DefaultReceiptOption = previous })
	hash := "0x" + strings.Repeat("12", 32)
	tests := []struct {
		input, args string
		reject      bool
	}{
		{`"latest_state"`, `["latest"]`, false},
		{`"earliest"`, `["0x0"]`, false},
		{`"0x1"`, `["0x1"]`, false},
		{`"0x7fffffffffffffff"`, `["0x7fffffffffffffff"]`, false},
		{`{"blockHash":"` + hash + `","requirePivot":true}`, `[{"blockHash":"` + hash + `","requireCanonical":true}]`, false},
		{`"latest_confirmed"`, ``, true},
		{`"0x8000000000000000"`, ``, true},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			p := &bridgeTestProvider{result: `[]`}
			api := NewCfxAPI(web3go.NewClientWithProvider(p), 1030, nil)
			response := bridgeRequest(t, api, "cfx_getEpochReceipts", `[`+test.input+`]`)
			if test.reject {
				require.NotNil(t, response["error"])
				require.Zero(t, p.calls)
				return
			}
			require.Nil(t, response["error"])
			require.JSONEq(t, `[[]]`, string(response["result"]))
			require.Equal(t, "eth_getBlockReceipts", p.method)
			require.Equal(t, 1, p.calls)
			args, err := json.Marshal(p.args)
			require.NoError(t, err)
			require.JSONEq(t, test.args, string(args))
			p.err = fmt.Errorf("fixture receipt failure")
			response = bridgeRequest(t, api, "cfx_getEpochReceipts", `[`+test.input+`]`)
			require.Nil(t, response["result"])
			require.Contains(t, string(response["error"]), p.err.Error())
		})
	}
}
