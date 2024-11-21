package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

type cfxDebugAPI struct {
	stateHandler *handler.CfxStateHandler
}

func (api *cfxDebugAPI) GetEpochReceiptProofByTransaction(ctx context.Context, hash types.Hash) (proof *types.EpochReceiptProof, err error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.DebugGetEpochReceiptProofByTransaction(ctx, cfx, hash)
}

func (api *cfxDebugAPI) GetTransactionsByEpoch(
	ctx context.Context, epoch types.Epoch) (wrapTransactions []types.WrapTransaction, err error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.DebugGetTransactionsByEpoch(ctx, cfx, epoch)
}

func (api *cfxDebugAPI) GetTransactionsByBlock(ctx context.Context, hash types.Hash) (wrapTransactions []types.WrapTransaction, err error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.DebugGetTransactionsByBlock(ctx, cfx, hash)
}
