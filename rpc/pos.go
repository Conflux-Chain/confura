package rpc

import (
	"context"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	postypes "github.com/Conflux-Chain/go-conflux-sdk/types/pos"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var (
	// flyweight objects
	emptyPosStatus        = &postypes.Status{}
	emptyPosCommitteState = &postypes.CommitteeState{}
)

type posAPI struct {
	provider *node.ClientProvider
}

func newPosAPI(provider *node.ClientProvider) *posAPI {
	return &posAPI{provider: provider}
}

func (api *posAPI) GetStatus(ctx context.Context) (*postypes.Status, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return emptyPosStatus, err
	}

	status, err := cfx.Pos().GetStatus()
	return &status, err
}

func (api *posAPI) GetAccount(
	ctx context.Context, address postypes.Address, view ...hexutil.Uint64,
) (*postypes.Account, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	account, err := cfx.Pos().GetAccount(address, view...)
	return &account, err
}

func (api *posAPI) GetCommittee(
	ctx context.Context, view ...hexutil.Uint64,
) (*postypes.CommitteeState, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return emptyPosCommitteState, err
	}

	committeeSate, err := cfx.Pos().GetCommittee(view...)
	return &committeeSate, err
}

func (api *posAPI) GetBlockByHash(
	ctx context.Context, blockHash types.Hash,
) (*postypes.Block, error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	posBlock, err := cfx.Pos().GetBlockByHash(blockHash)
	return posBlock, err
}

func (api *posAPI) GetBlockByNumber(
	ctx context.Context, blockNumber postypes.BlockNumber,
) (block *postypes.Block, err error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	posBlock, err := cfx.Pos().GetBlockByNumber(blockNumber)
	return posBlock, err
}

func (api *posAPI) GetTransactionByNumber(
	ctx context.Context, txNumber hexutil.Uint64,
) (transaction *postypes.Transaction, err error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	posTx, err := cfx.Pos().GetTransactionByNumber(txNumber)
	return posTx, err
}

func (api *posAPI) GetRewardsByEpoch(
	ctx context.Context, epochNumber hexutil.Uint64,
) (reward *postypes.EpochReward, err error) {
	cfx, err := api.provider.GetClientByIP(ctx)
	if err != nil {
		return nil, err
	}

	epochRewards, err := cfx.Pos().GetRewardsByEpoch(epochNumber)
	return &epochRewards, err
}
