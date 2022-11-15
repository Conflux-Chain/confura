package cfxbridge

import (
	"context"

	"github.com/ethereum/go-ethereum/common/hexutil"
	w3rpc "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
)

type TxpoolAPI struct {
	ethClient *web3go.Client
}

func NewTxpoolAPI(ethClient *web3go.Client) *TxpoolAPI {
	return &TxpoolAPI{ethClient}
}

func (api *TxpoolAPI) NextNonce(ctx context.Context, address EthAddress) (val *hexutil.Big, err error) {
	pendingBlock := types.BlockNumberOrHashWithNumber(w3rpc.PendingBlockNumber)

	nonce, err := api.ethClient.Eth.TransactionCount(address.value, &pendingBlock)
	if err != nil {
		return nil, err
	}

	return (*hexutil.Big)(nonce), nil
}
