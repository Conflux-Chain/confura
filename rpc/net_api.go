package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/node"
)

type netAPI struct {
	provider *node.EthClientProvider
}

func newNetAPI(provider *node.EthClientProvider) *netAPI {
	return &netAPI{provider: provider}
}

// Version returns the current network id.
func (api *netAPI) Version(ctx context.Context) (string, error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return "", err
	}

	return w3c.Eth.NetVersion()
}
