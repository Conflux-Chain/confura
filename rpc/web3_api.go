package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/node"
)

type web3API struct {
	provider *node.ClientProvider
}

func newWeb3API(provider *node.ClientProvider) *web3API {
	return &web3API{provider}
}

// ClientVersion returns the current client version.
func (api *web3API) ClientVersion(ctx context.Context) (string, error) {
	// TODO: add implementation
	return "", nil
}
