package acl

import (
	"context"
	"testing"

	cfxTypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	allowedContractAddr    = "0x2000000000000000000000000000000000000002"
	notAllowedContractAddr = "0x3000000000000000000000000000000000000003"
)

func ethGetLogsContext(fq web3Types.FilterQuery) Context {
	return Context{
		Context:   context.Background(),
		RpcMethod: "eth_getLogs",
		ExtractRpcParams: func() ([]interface{}, error) {
			return []interface{}{fq}, nil
		},
	}
}

func cfxGetLogsContext(fq cfxTypes.LogFilter) Context {
	return Context{
		Context:   context.Background(),
		RpcMethod: "cfx_getLogs",
		ExtractRpcParams: func() ([]interface{}, error) {
			return []interface{}{fq}, nil
		},
	}
}

func TestEthGetLogsAllowlist(t *testing.T) {
	v := NewEthValidator(&AllowList{ContractAddresses: []string{allowedContractAddr}})

	// A filter targeting an allowlisted contract is permitted.
	err := v.Validate(ethGetLogsContext(web3Types.FilterQuery{
		Addresses: []common.Address{common.HexToAddress(allowedContractAddr)},
	}))
	require.NoError(t, err)

	// A filter targeting a non-allowlisted contract is rejected.
	err = v.Validate(ethGetLogsContext(web3Types.FilterQuery{
		Addresses: []common.Address{common.HexToAddress(notAllowedContractAddr)},
	}))
	require.ErrorIs(t, err, errInvalidContractAddr)

	// A filter that constrains no contract address would match every contract, so it
	// must not slip past the allowlist.
	err = v.Validate(ethGetLogsContext(web3Types.FilterQuery{}))
	require.ErrorIs(t, err, errInvalidContractAddr)
}

func TestEthGetLogsWithoutAllowlistIsUnrestricted(t *testing.T) {
	v := NewEthValidator(&AllowList{})

	err := v.Validate(ethGetLogsContext(web3Types.FilterQuery{}))
	assert.NoError(t, err)
}

func TestCfxGetLogsAllowlist(t *testing.T) {
	allowed := cfxaddress.MustNew(allowedContractAddr, 1)
	v := NewCfxValidator(&AllowList{ContractAddresses: []string{allowed.MustGetBase32Address()}})

	// A filter targeting an allowlisted contract is permitted.
	err := v.Validate(cfxGetLogsContext(cfxTypes.LogFilter{
		Address: []cfxaddress.Address{allowed},
	}))
	require.NoError(t, err)

	// An address-less filter must not bypass the allowlist.
	err = v.Validate(cfxGetLogsContext(cfxTypes.LogFilter{}))
	require.ErrorIs(t, err, errInvalidContractAddr)
}
