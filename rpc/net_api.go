package rpc

import (
	"context"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

// netAPI provides evm space net RPC proxy API.
type netAPI struct{}

// Version returns the current network id.
func (api *netAPI) Version(ctx context.Context) (string, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.NetVersion()
}

// Listening returns true if client is actively listening for network connections.
func (api *netAPI) Listening(ctx context.Context) (res bool, err error) {
	w3c := GetEthClientFromContext(ctx)
	err = w3c.Client.CallContext(ctx, &res, "net_listening")
	return res, err
}

// PeerCount Returns number of peers currently connected to the client.
func (api *netAPI) PeerCount(ctx context.Context) (res hexutil.Uint64, err error) {
	w3c := GetEthClientFromContext(ctx)
	err = w3c.Client.CallContext(ctx, &res, "net_peerCount")
	return res, err
}
