package handler

import (
	"context"
	"strings"

	"github.com/Conflux-Chain/confura/node"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	postypes "github.com/Conflux-Chain/go-conflux-sdk/types/pos"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

const (
	errPatternStateNotReady = "state is not ready"
	errPatternStateNotExist = "out-of-bound StateAvailabilityBoundary"
)

// CfxStateHandler handles core space state RPC method by redirecting requests to another
// full state node if state is not available on normal full node.
type CfxStateHandler struct {
	cp *node.CfxClientProvider
}

func NewCfxStateHandler(cp *node.CfxClientProvider) *CfxStateHandler {
	return &CfxStateHandler{cp: cp}
}

func (h *CfxStateHandler) GetBalance(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	epoch ...*types.EpochOrBlockHash,
) (*hexutil.Big, error) {
	bal, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetBalance(address, epoch...)
	})

	if err != nil {
		return nil, err
	}

	return bal.(*hexutil.Big), err
}

func (h *CfxStateHandler) GetNextNonce(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	epoch ...*types.EpochOrBlockHash,
) (*hexutil.Big, error) {
	nonce, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetNextNonce(address, epoch...)
	})

	if err != nil {
		return nil, err
	}

	return nonce.(*hexutil.Big), err
}

func (h *CfxStateHandler) GetStorageAt(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	position *hexutil.Big,
	epoch ...*types.EpochOrBlockHash,
) (hexutil.Bytes, error) {
	storage, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetStorageAt(address, position, epoch...)
	})

	if err != nil {
		return hexutil.Bytes{}, err
	}

	return storage.(hexutil.Bytes), err
}

func (h *CfxStateHandler) GetCode(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	epoch ...*types.EpochOrBlockHash,
) (hexutil.Bytes, error) {
	code, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetCode(address, epoch...)
	})

	if err != nil {
		return hexutil.Bytes{}, err
	}

	return code.(hexutil.Bytes), err
}

func (h *CfxStateHandler) Call(
	ctx context.Context,
	cfx sdk.ClientOperator,
	request types.CallRequest,
	epoch *types.EpochOrBlockHash,
) (hexutil.Bytes, error) {
	result, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Call(request, epoch)
	})

	if err != nil {
		return hexutil.Bytes{}, err
	}

	return result.(hexutil.Bytes), err
}

func (h *CfxStateHandler) EstimateGasAndCollateral(
	ctx context.Context,
	cfx sdk.ClientOperator,
	request types.CallRequest,
	epoch ...*types.Epoch,
) (types.Estimate, error) {
	est, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.EstimateGasAndCollateral(request, epoch...)
	})

	if err != nil {
		return types.Estimate{}, err
	}

	return est.(types.Estimate), err
}

func (h *CfxStateHandler) GetAdmin(
	ctx context.Context,
	cfx sdk.ClientOperator,
	contractAddress types.Address,
	epoch ...*types.Epoch,
) (*types.Address, error) {
	admin, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetAdmin(contractAddress, epoch...)
	})

	if err != nil {
		return nil, err
	}

	return admin.(*types.Address), err
}

func (h *CfxStateHandler) GetSponsorInfo(
	ctx context.Context,
	cfx sdk.ClientOperator,
	contractAddress types.Address,
	epoch ...*types.Epoch,
) (sponsor types.SponsorInfo, err error) {
	info, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetSponsorInfo(contractAddress, epoch...)
	})

	if err != nil {
		return types.SponsorInfo{}, err
	}

	return info.(types.SponsorInfo), err
}

func (h *CfxStateHandler) GetStakingBalance(
	ctx context.Context,
	cfx sdk.ClientOperator,
	account types.Address,
	epoch ...*types.Epoch,
) (*hexutil.Big, error) {
	bal, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetStakingBalance(account, epoch...)
	})

	if err != nil {
		return nil, err
	}

	return bal.(*hexutil.Big), err
}

func (h *CfxStateHandler) GetDepositList(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	epoch ...*types.Epoch,
) ([]types.DepositInfo, error) {
	info, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetDepositList(address, epoch...)
	})

	if err != nil {
		return []types.DepositInfo{}, err
	}

	return info.([]types.DepositInfo), err
}

func (h *CfxStateHandler) GetVoteList(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	epoch ...*types.Epoch,
) ([]types.VoteStakeInfo, error) {
	info, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetVoteList(address, epoch...)
	})

	if err != nil {
		return []types.VoteStakeInfo{}, err
	}

	return info.([]types.VoteStakeInfo), err
}

func (h *CfxStateHandler) GetCollateralInfo(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (types.StorageCollateralInfo, error) {
	info, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetCollateralInfo(epoch...)
	})

	if err != nil {
		return types.StorageCollateralInfo{}, err
	}

	return info.(types.StorageCollateralInfo), err
}

func (h *CfxStateHandler) GetCollateralForStorage(
	ctx context.Context,
	cfx sdk.ClientOperator,
	account types.Address,
	epoch ...*types.Epoch,
) (*hexutil.Big, error) {
	storage, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetCollateralForStorage(account, epoch...)
	})

	if err != nil {
		return nil, err
	}

	return storage.(*hexutil.Big), err
}

func (h *CfxStateHandler) GetStorageRoot(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	epoch ...*types.Epoch,
) (*types.StorageRoot, error) {
	root, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetStorageRoot(address, epoch...)
	})

	if err != nil {
		return nil, err
	}

	return root.(*types.StorageRoot), err
}

func (h *CfxStateHandler) CheckBalanceAgainstTransaction(
	ctx context.Context,
	cfx sdk.ClientOperator,
	accountAddress types.Address,
	contractAddress types.Address,
	gasLimit *hexutil.Big,
	gasPrice *hexutil.Big,
	storageLimit *hexutil.Big,
	epoch ...*types.Epoch,
) (types.CheckBalanceAgainstTransactionResponse, error) {
	check, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.CheckBalanceAgainstTransaction(
			accountAddress, contractAddress, gasLimit, gasPrice, storageLimit, epoch...,
		)
	})

	if err != nil {
		return types.CheckBalanceAgainstTransactionResponse{}, err
	}

	return check.(types.CheckBalanceAgainstTransactionResponse), err
}

func (h *CfxStateHandler) GetAccountInfo(
	ctx context.Context,
	cfx sdk.ClientOperator,
	account types.Address,
	epoch ...*types.Epoch,
) (types.AccountInfo, error) {
	info, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetAccountInfo(account, epoch...)
	})

	if err != nil {
		return types.AccountInfo{}, err
	}

	return info.(types.AccountInfo), err
}

func (h *CfxStateHandler) GetInterestRate(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (*hexutil.Big, error) {
	rate, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetInterestRate(epoch...)
	})

	if err != nil {
		return nil, err
	}

	return rate.(*hexutil.Big), err
}

func (h *CfxStateHandler) GetAccumulateInterestRate(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (*hexutil.Big, error) {
	rate, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetAccumulateInterestRate(epoch...)
	})

	if err != nil {
		return nil, err
	}

	return rate.(*hexutil.Big), err
}

func (h *CfxStateHandler) GetSupplyInfo(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (types.TokenSupplyInfo, error) {
	info, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetSupplyInfo(epoch...)
	})

	if err != nil {
		return types.TokenSupplyInfo{}, err
	}

	return info.(types.TokenSupplyInfo), err
}

func (h *CfxStateHandler) GetPoSEconomics(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (types.PoSEconomics, error) {
	economics, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetPoSEconomics(epoch...)
	})

	if err != nil {
		return types.PoSEconomics{}, err
	}

	return economics.(types.PoSEconomics), err
}

func (h *CfxStateHandler) GetParamsFromVote(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (postypes.VoteParamsInfo, error) {
	vote, err := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetParamsFromVote(epoch...)
	})

	if err != nil {
		return postypes.VoteParamsInfo{}, err
	}

	return vote.(postypes.VoteParamsInfo), err
}

func (h *CfxStateHandler) doRequest(
	ctx context.Context,
	initCfx sdk.ClientOperator,
	clientFunc func(cfx sdk.ClientOperator) (interface{}, error),
) (interface{}, error) {
	result, err := clientFunc(initCfx)
	if err == nil || !isStateNotAvailable(err) {
		return result, err
	}

	fsCfx, cperr := h.cp.GetClientByIP(ctx, node.GroupCfxFullState)
	if cperr != nil {
		return result, err
	}

	return clientFunc(fsCfx)
}

func isStateNotAvailable(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), errPatternStateNotReady) ||
		strings.Contains(err.Error(), errPatternStateNotExist)
}
