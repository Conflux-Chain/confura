package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/node"
)

type netAPI struct {
	provider *node.ClientProvider
}

func newNetAPI(provider *node.ClientProvider) *netAPI {
	return &netAPI{provider}
}

// Version returns the current network id.
func (api *netAPI) Version(ctx context.Context) (string, error) {
	// TODO: add implementation
	return "", nil
}
