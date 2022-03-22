package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/node"
)

type web3API struct {
	provider *node.EthClientProvider
}

func newWeb3API(provider *node.EthClientProvider) *web3API {
	return &web3API{provider: provider}
}

// ClientVersion returns the current client version.
func (api *web3API) ClientVersion(ctx context.Context) (string, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return "", err
	}

	return w3c.Eth.ClientVersion()
}
