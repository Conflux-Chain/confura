package rpc

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type txPoolAPI struct {
	provider *node.ClientProvider
}

func (api *txPoolAPI) NextNonce(ctx context.Context, address types.Address) (
	val *hexutil.Big, err error,
) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return cfx.TxPool().NextNonce(address)
}
