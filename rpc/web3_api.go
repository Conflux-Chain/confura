package rpc

import (
	"context"

	"github.com/openweb3/web3go"
)

type web3API struct {
	w3c *web3go.Client
}

func newWeb3API(eth *web3go.Client) *web3API {
	return &web3API{w3c: eth}
}

// ClientVersion returns the current client version.
func (api *web3API) ClientVersion(ctx context.Context) (string, error) {
	return api.w3c.Eth.ClientVersion()
}
