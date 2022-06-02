package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/rpc/cache"
)

type netAPI struct{}

// Version returns the current network id.
func (api *netAPI) Version(ctx context.Context) (string, error) {
	w3c := GetEthClientFromContext(ctx)
	return cache.EthDefault.GetNetVersion(w3c.Client)
}
