package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/rpc/cache"
)

// netAPI provides evm space net RPC proxy API.
type netAPI struct{}

// Version returns the current network id.
func (api *netAPI) Version(ctx context.Context) (string, error) {
	w3c := GetEthClientFromContext(ctx)
	return cache.EthDefault.GetNetVersion(w3c.Client)
}
