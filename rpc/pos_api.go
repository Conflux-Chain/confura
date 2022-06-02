package rpc

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	postypes "github.com/Conflux-Chain/go-conflux-sdk/types/pos"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type posAPI struct{}

func (api *posAPI) GetStatus(ctx context.Context) (postypes.Status, error) {
	return GetCfxClientFromContext(ctx).Pos().GetStatus()
}

func (api *posAPI) GetAccount(ctx context.Context, address postypes.Address, view ...hexutil.Uint64) (postypes.Account, error) {
	return GetCfxClientFromContext(ctx).Pos().GetAccount(address, view...)
}

func (api *posAPI) GetCommittee(ctx context.Context, view ...hexutil.Uint64) (postypes.CommitteeState, error) {
	return GetCfxClientFromContext(ctx).Pos().GetCommittee(view...)
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
	return GetCfxClientFromContext(ctx).Pos().GetRewardsByEpoch(epochNumber)
}
