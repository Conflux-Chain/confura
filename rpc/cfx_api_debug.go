package rpc

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

func (api *cfxAPI) GetEpochReceipts(
	ctx context.Context, epoch types.EpochOrBlockHash, includeEthRecepits ...bool,
) (receipts [][]types.TransactionReceipt, err error) {
	return GetCfxClientFromContext(ctx).GetEpochReceipts(epoch, includeEthRecepits...)
}
