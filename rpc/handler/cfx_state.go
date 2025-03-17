package handler

import (
	"context"
	"strings"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/util/metrics"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
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
	bal, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetBalance(address, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getBalance", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return bal.(*hexutil.Big), nil
}

func (h *CfxStateHandler) GetNextNonce(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	epoch ...*types.EpochOrBlockHash,
) (*hexutil.Big, error) {
	nonce, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetNextNonce(address, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getNextNonce", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return nonce.(*hexutil.Big), nil
}

func (h *CfxStateHandler) GetStorageAt(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	position *hexutil.Big,
	epoch ...*types.EpochOrBlockHash,
) (hexutil.Bytes, error) {
	storage, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetStorageAt(address, position, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getStorageAt", "fullState").Mark(usefs)

	if err != nil {
		return hexutil.Bytes{}, err
	}

	return storage.(hexutil.Bytes), nil
}

func (h *CfxStateHandler) GetCode(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	epoch ...*types.EpochOrBlockHash,
) (hexutil.Bytes, error) {
	code, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetCode(address, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getCode", "fullState").Mark(usefs)

	if err != nil {
		return hexutil.Bytes{}, err
	}

	return code.(hexutil.Bytes), nil
}

func (h *CfxStateHandler) Call(
	ctx context.Context,
	cfx sdk.ClientOperator,
	request types.CallRequest,
	epoch *types.EpochOrBlockHash,
) (hexutil.Bytes, error) {
	result, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Call(request, epoch)
	})

	metrics.Registry.RPC.Percentage("cfx_call", "fullState").Mark(usefs)

	if err != nil {
		return hexutil.Bytes{}, err
	}

	return result.(hexutil.Bytes), nil
}

func (h *CfxStateHandler) EstimateGasAndCollateral(
	ctx context.Context,
	cfx sdk.ClientOperator,
	request types.CallRequest,
	epoch ...*types.Epoch,
) (types.Estimate, error) {
	est, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.EstimateGasAndCollateral(request, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_estimateGasAndCollateral", "fullState").Mark(usefs)

	if err != nil {
		return types.Estimate{}, err
	}

	return est.(types.Estimate), nil
}

func (h *CfxStateHandler) GetAdmin(
	ctx context.Context,
	cfx sdk.ClientOperator,
	contractAddress types.Address,
	epoch ...*types.Epoch,
) (*types.Address, error) {
	admin, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetAdmin(contractAddress, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getAdmin", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return admin.(*types.Address), nil
}

func (h *CfxStateHandler) GetSponsorInfo(
	ctx context.Context,
	cfx sdk.ClientOperator,
	contractAddress types.Address,
	epoch ...*types.Epoch,
) (sponsor types.SponsorInfo, err error) {
	info, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetSponsorInfo(contractAddress, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getSponsorInfo", "fullState").Mark(usefs)

	if err != nil {
		return types.SponsorInfo{}, err
	}

	return info.(types.SponsorInfo), nil
}

func (h *CfxStateHandler) GetStakingBalance(
	ctx context.Context,
	cfx sdk.ClientOperator,
	account types.Address,
	epoch ...*types.Epoch,
) (*hexutil.Big, error) {
	bal, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetStakingBalance(account, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getStakingBalance", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return bal.(*hexutil.Big), nil
}

func (h *CfxStateHandler) GetDepositList(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	epoch ...*types.Epoch,
) ([]types.DepositInfo, error) {
	info, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetDepositList(address, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getDepositList", "fullState").Mark(usefs)

	if err != nil {
		return []types.DepositInfo{}, err
	}

	return info.([]types.DepositInfo), nil
}

func (h *CfxStateHandler) GetVoteList(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	epoch ...*types.Epoch,
) ([]types.VoteStakeInfo, error) {
	info, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetVoteList(address, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getVoteList", "fullState").Mark(usefs)

	if err != nil {
		return []types.VoteStakeInfo{}, err
	}

	return info.([]types.VoteStakeInfo), nil
}

func (h *CfxStateHandler) GetCollateralInfo(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (types.StorageCollateralInfo, error) {
	info, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetCollateralInfo(epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getCollateralInfo", "fullState").Mark(usefs)

	if err != nil {
		return types.StorageCollateralInfo{}, err
	}

	return info.(types.StorageCollateralInfo), nil
}

func (h *CfxStateHandler) GetCollateralForStorage(
	ctx context.Context,
	cfx sdk.ClientOperator,
	account types.Address,
	epoch ...*types.Epoch,
) (*hexutil.Big, error) {
	storage, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetCollateralForStorage(account, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getCollateralForStorage", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return storage.(*hexutil.Big), nil
}

func (h *CfxStateHandler) GetStorageRoot(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address types.Address,
	epoch ...*types.Epoch,
) (*types.StorageRoot, error) {
	root, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetStorageRoot(address, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getStorageRoot", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return root.(*types.StorageRoot), nil
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
	check, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.CheckBalanceAgainstTransaction(
			accountAddress, contractAddress, gasLimit, gasPrice, storageLimit, epoch...,
		)
	})

	metrics.Registry.RPC.Percentage("cfx_checkBalanceAgainstTransaction", "fullState").Mark(usefs)

	if err != nil {
		return types.CheckBalanceAgainstTransactionResponse{}, err
	}

	return check.(types.CheckBalanceAgainstTransactionResponse), nil
}

func (h *CfxStateHandler) GetAccountInfo(
	ctx context.Context,
	cfx sdk.ClientOperator,
	account types.Address,
	epoch ...*types.Epoch,
) (types.AccountInfo, error) {
	info, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetAccountInfo(account, epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getAccount", "fullState").Mark(usefs)

	if err != nil {
		return types.AccountInfo{}, err
	}

	return info.(types.AccountInfo), nil
}

func (h *CfxStateHandler) GetInterestRate(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (*hexutil.Big, error) {
	rate, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetInterestRate(epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getInterestRate", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return rate.(*hexutil.Big), nil
}

func (h *CfxStateHandler) GetAccumulateInterestRate(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (*hexutil.Big, error) {
	rate, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetAccumulateInterestRate(epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getAccumulateInterestRate", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return rate.(*hexutil.Big), nil
}

func (h *CfxStateHandler) GetSupplyInfo(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (types.TokenSupplyInfo, error) {
	info, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetSupplyInfo(epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getSupplyInfo", "fullState").Mark(usefs)

	if err != nil {
		return types.TokenSupplyInfo{}, err
	}

	return info.(types.TokenSupplyInfo), nil
}

func (h *CfxStateHandler) GetPoSEconomics(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (types.PoSEconomics, error) {
	economics, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetPoSEconomics(epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getPoSEconomics", "fullState").Mark(usefs)

	if err != nil {
		return types.PoSEconomics{}, err
	}

	return economics.(types.PoSEconomics), nil
}

func (h *CfxStateHandler) GetParamsFromVote(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch ...*types.Epoch,
) (postypes.VoteParamsInfo, error) {
	vote, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.GetParamsFromVote(epoch...)
	})

	metrics.Registry.RPC.Percentage("cfx_getParamsFromVote", "fullState").Mark(usefs)

	if err != nil {
		return postypes.VoteParamsInfo{}, err
	}

	return vote.(postypes.VoteParamsInfo), nil
}

func (h *CfxStateHandler) PosGetAccount(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address postypes.Address,
	view ...hexutil.Uint64,
) (postypes.Account, error) {
	account, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Pos().GetAccount(address, view...)
	})

	metrics.Registry.RPC.Percentage("pos_getAccount", "fullState").Mark(usefs)

	if err != nil {
		return postypes.Account{}, err
	}

	return account.(postypes.Account), nil
}

func (h *CfxStateHandler) PosGetAccountByPowAddress(
	ctx context.Context,
	cfx sdk.ClientOperator,
	address cfxaddress.Address,
	blockNumber ...hexutil.Uint64,
) (postypes.Account, error) {
	account, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Pos().GetAccountByPowAddress(address, blockNumber...)
	})

	metrics.Registry.RPC.Percentage("pos_getAccountByPowAddress", "fullState").Mark(usefs)

	if err != nil {
		return postypes.Account{}, err
	}

	return account.(postypes.Account), nil
}

func (h *CfxStateHandler) PosGetCommittee(
	ctx context.Context,
	cfx sdk.ClientOperator,
	view ...hexutil.Uint64,
) (postypes.CommitteeState, error) {
	committee, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Pos().GetCommittee(view...)
	})

	metrics.Registry.RPC.Percentage("pos_getCommittee", "fullState").Mark(usefs)

	if err != nil {
		return postypes.CommitteeState{}, err
	}

	return committee.(postypes.CommitteeState), nil
}

func (h *CfxStateHandler) PosGetRewardsByEpoch(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epochNumber hexutil.Uint64,
) (postypes.EpochReward, error) {
	reward, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Pos().GetRewardsByEpoch(epochNumber)
	})

	metrics.Registry.RPC.Percentage("pos_getRewardsByEpoch", "fullState").Mark(usefs)

	if err != nil {
		return postypes.EpochReward{}, err
	}

	return reward.(postypes.EpochReward), nil
}

func (h *CfxStateHandler) PosGetEpochState(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epochNumber hexutil.Uint64,
) (*postypes.EpochState, error) {
	state, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Pos().GetEpochState(epochNumber)
	})

	metrics.Registry.RPC.Percentage("pos_getEpochState", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return state.(*postypes.EpochState), nil
}

func (h *CfxStateHandler) PosGetLedgerInfoByEpoch(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epochNumber hexutil.Uint64,
) (*postypes.LedgerInfoWithSignatures, error) {
	info, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Pos().GetLedgerInfoByEpoch(epochNumber)
	})

	metrics.Registry.RPC.Percentage("pos_getLedgerInfoByEpoch", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return info.(*postypes.LedgerInfoWithSignatures), nil
}

func (h *CfxStateHandler) PosGetLedgerInfosByEpoch(
	ctx context.Context,
	cfx sdk.ClientOperator,
	startEpoch hexutil.Uint64,
	endEpoch hexutil.Uint64,
) ([]*postypes.LedgerInfoWithSignatures, error) {
	info, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Pos().GetLedgerInfosByEpoch(startEpoch, endEpoch)
	})

	metrics.Registry.RPC.Percentage("pos_getLedgerInfosByEpoch", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return info.([]*postypes.LedgerInfoWithSignatures), nil
}

func (h *CfxStateHandler) PosGetLedgerInfoByBlockNumber(
	ctx context.Context,
	cfx sdk.ClientOperator,
	blockNumber postypes.BlockNumber,
) (*postypes.LedgerInfoWithSignatures, error) {
	info, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Pos().GetLedgerInfoByBlockNumber(blockNumber)
	})

	metrics.Registry.RPC.Percentage("pos_getLedgerInfoByBlockNumber", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return info.(*postypes.LedgerInfoWithSignatures), nil
}

func (h *CfxStateHandler) PosGetLedgerInfoByEpochAndRound(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epochNumber hexutil.Uint64,
	round hexutil.Uint64,
) (*postypes.LedgerInfoWithSignatures, error) {
	info, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Pos().GetLedgerInfoByEpochAndRound(epochNumber, round)
	})

	metrics.Registry.RPC.Percentage("pos_getLedgerInfoByEpochAndRound", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return info.(*postypes.LedgerInfoWithSignatures), nil
}

func (h *CfxStateHandler) DebugGetEpochReceiptProofByTransaction(
	ctx context.Context,
	cfx sdk.ClientOperator,
	txHash types.Hash,
) (*types.EpochReceiptProof, error) {
	epochRcpt, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Debug().GetEpochReceiptProofByTransaction(txHash)
	})

	metrics.Registry.RPC.Percentage("debug_getEpochReceiptProofByTransaction", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return epochRcpt.(*types.EpochReceiptProof), nil
}

func (h *CfxStateHandler) DebugGetTransactionsByEpoch(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch types.Epoch,
) ([]types.WrapTransaction, error) {
	txns, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Debug().GetTransactionsByEpoch(epoch)
	})

	metrics.Registry.RPC.Percentage("debug_getTransactionsByEpoch", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return txns.([]types.WrapTransaction), nil
}

func (h *CfxStateHandler) DebugGetTransactionsByBlock(
	ctx context.Context,
	cfx sdk.ClientOperator,
	blockHash types.Hash,
) ([]types.WrapTransaction, error) {
	txns, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Debug().GetTransactionsByBlock(blockHash)
	})

	metrics.Registry.RPC.Percentage("debug_getTransactionsByBlock", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return txns.([]types.WrapTransaction), nil
}

func (h *CfxStateHandler) TraceBlock(
	ctx context.Context,
	cfx sdk.ClientOperator,
	blockHash types.Hash,
) (*types.LocalizedBlockTrace, error) {
	trace, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Trace().GetBlockTraces(blockHash)
	})

	metrics.Registry.RPC.Percentage("trace_block", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return trace.(*types.LocalizedBlockTrace), nil
}

func (h *CfxStateHandler) TraceFilter(
	ctx context.Context,
	cfx sdk.ClientOperator,
	filter types.TraceFilter,
) ([]types.LocalizedTrace, error) {
	trace, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Trace().FilterTraces(filter)
	})

	metrics.Registry.RPC.Percentage("trace_filter", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return trace.([]types.LocalizedTrace), nil
}

func (h *CfxStateHandler) TraceTransaction(
	ctx context.Context,
	cfx sdk.ClientOperator,
	txHash types.Hash,
) ([]types.LocalizedTrace, error) {
	trace, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Trace().GetTransactionTraces(txHash)
	})

	metrics.Registry.RPC.Percentage("trace_transaction", "fullState").Mark(usefs)

	if err != nil {
		return nil, err
	}

	return trace.([]types.LocalizedTrace), nil
}

func (h *CfxStateHandler) TraceEpoch(
	ctx context.Context,
	cfx sdk.ClientOperator,
	epoch types.Epoch,
) (types.EpochTrace, error) {
	trace, err, usefs := h.doRequest(ctx, cfx, func(cfx sdk.ClientOperator) (interface{}, error) {
		return cfx.Trace().GetEpochTraces(epoch)
	})

	metrics.Registry.RPC.Percentage("trace_epoch", "fullState").Mark(usefs)

	if err != nil {
		return types.EpochTrace{}, err
	}

	return trace.(types.EpochTrace), nil
}

func (h *CfxStateHandler) doRequest(
	ctx context.Context,
	initCfx sdk.ClientOperator,
	clientFunc func(cfx sdk.ClientOperator) (interface{}, error),
) (interface{}, error, bool) {
	result, err := clientFunc(initCfx)
	if err == nil || !isStateNotAvailable(err) {
		return result, err, false
	}

	fsCfx, cperr := h.cp.GetClientByIP(ctx, node.GroupCfxFullState)
	if cperr == nil {
		result, err = clientFunc(fsCfx)
	}

	return result, err, true
}

func isStateNotAvailable(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), errPatternStateNotReady) ||
		strings.Contains(err.Error(), errPatternStateNotExist)
}
