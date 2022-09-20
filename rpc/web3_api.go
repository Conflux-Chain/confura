package rpc

import (
	"context"

	"github.com/scroll-tech/rpc-gateway/rpc/cache"
)

// web3API provides evm space web3 RPC proxy API.
type web3API struct{}

// ClientVersion returns the current client version.
func (api *web3API) ClientVersion(ctx context.Context) (string, error) {
	w3c := GetEthClientFromContext(ctx)
	return cache.EthDefault.GetClientVersion(w3c.Client)
}
