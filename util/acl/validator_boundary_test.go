package acl

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func TestContractACLFilterAndSubscriptionBoundaries(t *testing.T) {
	v := NewEthValidator(&AllowList{ContractAddresses: []string{allowedContractAddr}})
	for _, method := range []string{"eth_getLogs", "eth_newFilter"} {
		ctx := ethGetLogsContext(web3Types.FilterQuery{Addresses: []common.Address{common.HexToAddress(allowedContractAddr)}})
		ctx.RpcMethod = method
		require.NoError(t, v.Validate(ctx))
		ctx.ExtractRpcParams = nil
		require.Error(t, v.Validate(ctx))
	}
	for _, tc := range []struct {
		params  string
		allowed bool
	}{
		{`["logs",{"address":"` + allowedContractAddr + `"}]`, true},
		{`["logs",{"address":"` + notAllowedContractAddr + `"}]`, false},
		{`["logs",{}]`, false}, {`["logs"]`, false}, {`["logs",null]`, false},
		{`["newHeads"]`, true}, {`[null]`, true},
	} {
		ctx := Context{Context: context.Background(), RpcMethod: "eth_subscribe", RawParams: json.RawMessage(tc.params)}
		err := v.Validate(ctx)
		require.Equal(t, tc.allowed, err == nil, tc.params)
	}
	for _, method := range []string{"eth_getFilterLogs", "eth_getFilterChanges"} {
		require.Error(t, v.Validate(Context{RpcMethod: method}))
		require.NoError(t, NewEthValidator(&AllowList{}).Validate(Context{RpcMethod: method}))
	}
}

func TestLiteralOriginAndCoreAddress(t *testing.T) {
	v := NewEthValidator(&AllowList{Origins: []string{"https://app.example.com"}})
	for _, origin := range []string{"https://app-example.com", "https://appXexample.com"} {
		require.Error(t, v.Validate(Context{Context: context.WithValue(context.Background(), handlers.CtxKeyReqOrigin, origin)}))
	}
	require.NoError(t, v.Validate(Context{Context: context.WithValue(context.Background(), handlers.CtxKeyReqOrigin, "https://app.example.com")}))
	addr := cfxaddress.MustNew(allowedContractAddr, 1)
	for _, form := range []string{addr.MustGetBase32Address(), addr.MustGetVerboseBase32Address()} {
		v := NewCfxValidator(&AllowList{ContractAddresses: []string{form}})
		for _, method := range []string{"cfx_getBalance", "cfx_getCode", "cfx_getStorageAt", "cfx_getNextNonce"} {
			require.NoError(t, v.Validate(Context{RpcMethod: method, ExtractRpcParams: func() ([]interface{}, error) { return []interface{}{addr}, nil }}))
		}
	}
}
