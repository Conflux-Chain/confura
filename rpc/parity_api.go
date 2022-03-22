package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/node"
	"github.com/openweb3/web3go/types"
)

type parityAPI struct {
	provider *node.EthClientProvider
}

func newParityAPI(provider *node.EthClientProvider) *parityAPI {
	return &parityAPI{provider: provider}
}

func (api *parityAPI) GetBlockReceipts(ctx context.Context, blockNumOrHash *types.BlockNumberOrHash) (val []types.Receipt, err error) {
	w3c, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	return w3c.Parity.BlockReceipts(blockNumOrHash)
}
