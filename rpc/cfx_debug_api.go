package rpc

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

type cfxDebugAPI struct{}

func (api *cfxDebugAPI) GetEpochReceiptProofByTransaction(ctx context.Context, hash types.Hash) (proof *types.EpochReceiptProof, err error) {
	return GetCfxClientFromContext(ctx).Debug().GetEpochReceiptProofByTransaction(hash)
}

func (api *cfxDebugAPI) GetTransactionsByEpoch(
	ctx context.Context, epoch types.Epoch) (wrapTransactions []types.WrapTransaction, err error) {
	return GetCfxClientFromContext(ctx).Debug().GetTransactionsByEpoch(epoch)
}

func (api *cfxDebugAPI) GetTransactionsByBlock(ctx context.Context, hash types.Hash) (wrapTransactions []types.WrapTransaction, err error) {
	return GetCfxClientFromContext(ctx).Debug().GetTransactionsByBlock(hash)
}
