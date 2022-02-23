package rpc

import (
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
)

type parityAPI struct {
	w3c *web3go.Client
}

func newParityAPI(eth *web3go.Client) *parityAPI {
	return &parityAPI{w3c: eth}
}

func (api *parityAPI) GetBlockReceipts(blockNumOrHash *types.BlockNumberOrHash) (val []types.Receipt, err error) {
	return api.w3c.Parity.BlockReceipts(blockNumOrHash)
}
