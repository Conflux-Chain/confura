package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/rpc/cache"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// web3API provides evm space web3 RPC proxy API.
type web3API struct{}

// ClientVersion returns the current client version.
func (api *web3API) ClientVersion(ctx context.Context) (string, error) {
	w3c := GetEthClientFromContext(ctx)
	return cache.EthDefault.GetClientVersion(w3c.Client)
}

// Sha3 returns Keccak-256 (not the standardized SHA3-256) hash of the given data.
func (api *web3API) Sha3(ctx context.Context, data hexutil.Bytes) (res hexutil.Bytes, err error) {
	w3c := GetEthClientFromContext(ctx)
	err = w3c.Client.CallContext(ctx, &res, "web3_sha3", data)
	return res, err
}
