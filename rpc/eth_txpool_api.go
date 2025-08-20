package rpc

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
)

type ethTxPoolAPI struct{}

func (*ethTxPoolAPI) Status(ctx context.Context) (*types.TxpoolStatus, error) {
	return GetEthClientFromContext(ctx).TxPool.TxpoolStatus()
}

func (*ethTxPoolAPI) Inspect(ctx context.Context) (*types.TxpoolInspect, error) {
	return GetEthClientFromContext(ctx).TxPool.TxpoolInspect()
}

func (*ethTxPoolAPI) ContentFrom(ctx context.Context, from common.Address) (*types.TxpoolContentFrom, error) {
	return GetEthClientFromContext(ctx).TxPool.TxpoolContentFrom(from)
}

func (*ethTxPoolAPI) Content(ctx context.Context) (*types.TxpoolContent, error) {
	return GetEthClientFromContext(ctx).TxPool.TxpoolContent()
}
