package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/rpc/cache"
)

type netAPI struct {
	provider *node.EthClientProvider

	cache *cache.EthCache
}

func newNetAPI(provider *node.EthClientProvider) *netAPI {
	return &netAPI{
		provider: provider,
		cache:    cache.NewEth(),
	}
}

// Version returns the current network id.
func (api *netAPI) Version(ctx context.Context) (string, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return "", err
	}

	return api.cache.GetNetVersion(w3c.Client)
}
