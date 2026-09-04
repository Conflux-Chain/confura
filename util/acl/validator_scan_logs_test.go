package acl_test

import (
	"testing"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/util/acl"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func TestEthScanLogsContractAllowlist(t *testing.T) {
	allowed := common.HexToAddress("0x1111111111111111111111111111111111111111")
	denied := common.HexToAddress("0x2222222222222222222222222222222222222222")
	validator := acl.NewEthValidator(&acl.AllowList{ContractAddresses: []string{allowed.String()}})

	for _, method := range []string{"eth_scanLogs", "eth_scanLogsWithPivotAssumption"} {
		t.Run(method, func(t *testing.T) {
			ctx := acl.Context{RpcMethod: method, ExtractRpcParams: func() ([]interface{}, error) {
				return []interface{}{handler.EthScanLogRequest{Filter: handler.EthScanLogFilter{Address: &allowed}}}, nil
			}}
			require.NoError(t, validator.Validate(ctx))

			ctx.ExtractRpcParams = func() ([]interface{}, error) {
				return []interface{}{handler.EthScanLogRequest{Filter: handler.EthScanLogFilter{Address: &denied}}}, nil
			}
			require.ErrorContains(t, validator.Validate(ctx), "invalid contract address")
		})
	}
}

func TestCfxScanLogsContractAllowlist(t *testing.T) {
	allowed := cfxaddress.MustNewFromHex("0x8111111111111111111111111111111111111111", 1029)
	denied := cfxaddress.MustNewFromHex("0x8222222222222222222222222222222222222222", 1029)
	validator := acl.NewCfxValidator(&acl.AllowList{ContractAddresses: []string{allowed.String()}})

	for _, method := range []string{"cfx_scanLogs", "cfx_scanLogsWithPivotAssumption"} {
		t.Run(method, func(t *testing.T) {
			ctx := acl.Context{RpcMethod: method, ExtractRpcParams: func() ([]interface{}, error) {
				return []interface{}{handler.CfxScanLogRequest{Filter: handler.CfxScanLogFilter{Address: &allowed}}}, nil
			}}
			require.NoError(t, validator.Validate(ctx))

			ctx.ExtractRpcParams = func() ([]interface{}, error) {
				return []interface{}{handler.CfxScanLogRequest{Filter: handler.CfxScanLogFilter{Address: &denied}}}, nil
			}
			require.ErrorContains(t, validator.Validate(ctx), "invalid contract address")
		})
	}
}

func TestScanLogsAllowlistAcceptsFiltersWithoutAddress(t *testing.T) {
	ethValidator := acl.NewEthValidator(&acl.AllowList{
		ContractAddresses: []string{common.HexToAddress("0x1111111111111111111111111111111111111111").String()},
	})
	for _, method := range []string{"eth_scanLogs", "eth_scanLogsWithPivotAssumption"} {
		for _, filter := range []handler.EthScanLogFilter{{}, {Topic0: ptr(common.HexToHash("0x1"))}} {
			ctx := acl.Context{RpcMethod: method, ExtractRpcParams: func() ([]interface{}, error) {
				return []interface{}{handler.EthScanLogRequest{Filter: filter}}, nil
			}}
			require.NoError(t, ethValidator.Validate(ctx))
		}
	}

	cfxValidator := acl.NewCfxValidator(&acl.AllowList{
		ContractAddresses: []string{cfxaddress.MustNewFromHex("0x8111111111111111111111111111111111111111", 1029).String()},
	})
	for _, method := range []string{"cfx_scanLogs", "cfx_scanLogsWithPivotAssumption"} {
		for _, filter := range []handler.CfxScanLogFilter{{}, {Topic0: ptr(cfxtypes.Hash("0x1"))}} {
			ctx := acl.Context{RpcMethod: method, ExtractRpcParams: func() ([]interface{}, error) {
				return []interface{}{handler.CfxScanLogRequest{Filter: filter}}, nil
			}}
			require.NoError(t, cfxValidator.Validate(ctx))
		}
	}
}

func ptr[T any](value T) *T { return &value }
