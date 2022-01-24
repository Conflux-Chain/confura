package rpc

import (
	"context"

	"github.com/openweb3/web3go"
)

type netAPI struct {
	w3c *web3go.Client
}

func newNetAPI(eth *web3go.Client) *netAPI {
	return &netAPI{w3c: eth}
}

// Version returns the current network id.
func (api *netAPI) Version(ctx context.Context) (string, error) {
	return api.w3c.Eth.NetVersion()
}
