package rpc

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

func (api *cfxAPI) GetEpochReceipts(ctx context.Context, epoch types.Epoch) ([][]types.TransactionReceipt, error) {
	cfx := GetCfxClientFromContext(ctx)

	if _, ok := epoch.ToInt(); !ok {
		tagOrHash := epoch.String()
		if len(tagOrHash) == 66 { // block hash
			return cfx.GetEpochReceiptsByPivotBlockHash(types.Hash(tagOrHash))
		}
	}

	return cfx.GetEpochReceipts(epoch)
}
