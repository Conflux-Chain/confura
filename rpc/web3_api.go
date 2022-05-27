package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/rpc/cache"
)

type web3API struct {
	provider *node.EthClientProvider

	cache *cache.EthCache
}

func newWeb3API(provider *node.EthClientProvider) *web3API {
	return &web3API{
		provider: provider,
		cache:    cache.NewEth(),
	}
}

// ClientVersion returns the current client version.
func (api *web3API) ClientVersion(ctx context.Context) (string, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return "", err
	}

	return api.cache.GetClientVersion(w3c.Client)
}
