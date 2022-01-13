package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/node"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type ethAPI struct {
	provider *node.ClientProvider
}

func newEthAPI(provider *node.ClientProvider) *ethAPI {
	return &ethAPI{provider: provider}
}

// ChainId returns the chainID value for transaction replay protection.
func (api *ethAPI) ChainId(ctx context.Context) (*hexutil.Big, error) {
	// TODO: add implementation
	return nil, nil
}

// BlockNumber returns the block number of the chain head.
func (api *ethAPI) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	// TODO: add implementation
	return 0, nil
}

// TODO add EVM space ETH apis
