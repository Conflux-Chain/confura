package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	postypes "github.com/Conflux-Chain/go-conflux-sdk/types/pos"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// posAPI provides core space POS RPC proxy API.
type posAPI struct {
	stateHandler *handler.CfxStateHandler
}

func (api *posAPI) GetStatus(ctx context.Context) (postypes.Status, error) {
	return GetCfxClientFromContext(ctx).Pos().GetStatus()
}

func (api *posAPI) GetAccount(ctx context.Context, address postypes.Address, view ...hexutil.Uint64) (postypes.Account, error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.PosGetAccount(ctx, cfx, address, view...)
}

// GetAccountByPowAddress returns pos account of pow address info at block
func (api *posAPI) GetAccountByPowAddress(
	ctx context.Context, address cfxaddress.Address, blockNumber ...hexutil.Uint64) (account postypes.Account, err error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.PosGetAccountByPowAddress(ctx, cfx, address, blockNumber...)
}

func (api *posAPI) GetCommittee(ctx context.Context, view ...hexutil.Uint64) (postypes.CommitteeState, error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.PosGetCommittee(ctx, cfx, view...)
}

func (api *posAPI) GetBlockByHash(ctx context.Context, blockHash types.Hash) (*postypes.Block, error) {
	return GetCfxClientFromContext(ctx).Pos().GetBlockByHash(blockHash)
}

func (api *posAPI) GetBlockByNumber(ctx context.Context, blockNumber postypes.BlockNumber) (*postypes.Block, error) {
	return GetCfxClientFromContext(ctx).Pos().GetBlockByNumber(blockNumber)
}

func (api *posAPI) GetTransactionByNumber(ctx context.Context, txNumber hexutil.Uint64) (*postypes.Transaction, error) {
	return GetCfxClientFromContext(ctx).Pos().GetTransactionByNumber(txNumber)
}

func (api *posAPI) GetRewardsByEpoch(ctx context.Context, epochNumber hexutil.Uint64) (postypes.EpochReward, error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.PosGetRewardsByEpoch(ctx, cfx, epochNumber)
}

func (api *posAPI) GetConsensusBlocks(ctx context.Context) (blocks []*postypes.Block, err error) {
	return GetCfxClientFromContext(ctx).Pos().GetConsensusBlocks()
}

func (api *posAPI) GetEpochState(ctx context.Context, epochNumber hexutil.Uint64) (epochState *postypes.EpochState, err error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.PosGetEpochState(ctx, cfx, epochNumber)
}

func (api *posAPI) GetLedgerInfoByEpoch(
	ctx context.Context, epochNumber hexutil.Uint64) (ledgerInfoWithSigs *postypes.LedgerInfoWithSignatures, err error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.PosGetLedgerInfoByEpoch(ctx, cfx, epochNumber)
}

func (api *posAPI) GetLedgerInfosByEpoch(
	ctx context.Context, startEpoch hexutil.Uint64, endEpoch hexutil.Uint64,
) (ledgerInfoWithSigs []*postypes.LedgerInfoWithSignatures, err error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.PosGetLedgerInfosByEpoch(ctx, cfx, startEpoch, endEpoch)
}

func (api *posAPI) GetLedgerInfoByBlockNumber(
	ctx context.Context, blockNumber postypes.BlockNumber) (ledgerInfoWithSigs *postypes.LedgerInfoWithSignatures, err error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.PosGetLedgerInfoByBlockNumber(ctx, cfx, blockNumber)
}

func (api *posAPI) GetLedgerInfoByEpochAndRound(
	ctx context.Context, epochNumber hexutil.Uint64, round hexutil.Uint64) (ledgerInfoWithSigs *postypes.LedgerInfoWithSignatures, err error) {
	cfx := GetCfxClientFromContext(ctx)
	return api.stateHandler.PosGetLedgerInfoByEpochAndRound(ctx, cfx, epochNumber, round)
}
