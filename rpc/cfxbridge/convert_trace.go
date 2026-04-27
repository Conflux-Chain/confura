package cfxbridge

import (
	"math/big"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3goTypes "github.com/openweb3/web3go/types"
)

const cfxTraceErrorReverted = "Reverted"

func convertTraceOutcome(ethTrace *web3goTypes.LocalizedTrace) types.OutcomeType {
	// success
	if ethTrace.Error == nil {
		return types.OUTCOME_SUCCESS
	}

	// reverted
	if *ethTrace.Error == cfxTraceErrorReverted {
		return types.OUTCOME_REVERTED
	}

	// fail
	return types.OUTCOME_FAIL
}

func convertTrace(ethTrace *web3goTypes.LocalizedTrace, cfxTraceType types.TraceType, cfxTraceAction interface{}) *types.LocalizedTrace {
	// transaction position
	var txPos hexutil.Uint64
	if ethTrace.TransactionPosition != nil {
		txPos = hexutil.Uint64(*ethTrace.TransactionPosition)
	}

	var valid bool
	if ethTrace.Valid != nil {
		valid = *ethTrace.Valid
	} else if ethTrace.Error == nil {
		valid = true
	}

	return &types.LocalizedTrace{
		Action:              cfxTraceAction,
		Valid:               valid,
		Type:                cfxTraceType,
		EpochHash:           types.Hash(ethTrace.BlockHash.Hex()),
		EpochNumber:         *types.NewBigInt(ethTrace.BlockNumber),
		BlockHash:           types.Hash(ethTrace.BlockHash.Hex()),
		TransactionPosition: txPos,
		TransactionHash:     types.Hash(ethTrace.TransactionHash.Hex()),
	}
}

func convertTraceCall(ethTrace *web3goTypes.LocalizedTrace, ethNetworkId uint32) (*types.LocalizedTrace, *types.LocalizedTrace) {
	ethActionCall := ethTrace.Action.(web3goTypes.Call)
	cfxTraceCall := convertTrace(ethTrace, types.TRACE_CALL, types.Call{
		Space:    types.SPACE_EVM,
		From:     ConvertAddress(ethActionCall.From, ethNetworkId),
		To:       ConvertAddress(ethActionCall.To, ethNetworkId),
		Value:    hexutil.Big(*ethActionCall.Value),
		Gas:      hexutil.Big(*ethActionCall.Gas),
		Input:    ethActionCall.Input,
		CallType: types.CallType(ethActionCall.CallType),
	})

	ethCallResult := ethTrace.Result.(web3goTypes.CallResult)
	gasLeft := big.NewInt(0).Sub(ethActionCall.Gas, ethCallResult.GasUsed)
	cfxTraceCallResult := convertTrace(ethTrace, types.TRACE_CALL_RESULT, types.CallResult{
		Outcome:    convertTraceOutcome(ethTrace),
		GasLeft:    hexutil.Big(*gasLeft),
		ReturnData: ethCallResult.Output,
	})

	return cfxTraceCall, cfxTraceCallResult
}

func convertTraceCreate(ethTrace *web3goTypes.LocalizedTrace, ethNetworkId uint32) (*types.LocalizedTrace, *types.LocalizedTrace) {
	ethActionCreate := ethTrace.Action.(web3goTypes.Create)
	createType := types.CREATE_NONE
	if ethActionCreate.CreateType != nil {
		createType = types.CreateType(*ethActionCreate.CreateType)
	}
	cfxTraceCreate := convertTrace(ethTrace, types.TRACE_CREATE, types.Create{
		Space:      types.SPACE_EVM,
		From:       ConvertAddress(ethActionCreate.From, ethNetworkId),
		Value:      hexutil.Big(*ethActionCreate.Value),
		Gas:        hexutil.Big(*ethActionCreate.Gas),
		Init:       ethActionCreate.Init,
		CreateType: createType,
	})

	ethCreateResult := ethTrace.Result.(web3goTypes.CreateResult)
	gasLeft := big.NewInt(0).Sub(ethActionCreate.Gas, ethCreateResult.GasUsed)
	cfxTraceCreateResult := convertTrace(ethTrace, types.TRACE_CREATE_RESULT, types.CreateResult{
		Outcome:    convertTraceOutcome(ethTrace),
		Addr:       ConvertAddress(ethCreateResult.Address, ethNetworkId),
		GasLeft:    hexutil.Big(*gasLeft),
		ReturnData: ethCreateResult.Code,
	})

	return cfxTraceCreate, cfxTraceCreateResult
}

func convertTraceSuicide(ethTrace *web3goTypes.LocalizedTrace, ethNetworkId uint32) *types.LocalizedTrace {
	ethActionSuicide := ethTrace.Action.(web3goTypes.Suicide)

	return convertTrace(ethTrace, types.TRACE_INTERNAL_TRANSFER_ACTIION, types.InternalTransferAction{
		From:       ConvertAddress(ethActionSuicide.Address, ethNetworkId),
		FromPocket: types.POCKET_BALANCE,
		FromSpace:  types.SPACE_EVM,
		To:         ConvertAddress(ethActionSuicide.RefundAddress, ethNetworkId),
		ToPocket:   types.POCKET_BALANCE,
		ToSpace:    types.SPACE_EVM,
		Value:      hexutil.Big(*ethActionSuicide.Balance),
	})
}

func ConvertTrace(ethTrace *web3goTypes.LocalizedTrace, ethNetworkId uint32) (*types.LocalizedTrace, *types.LocalizedTrace) {
	if ethTrace == nil {
		return nil, nil
	}

	switch ethTrace.Type {
	case web3goTypes.TRACE_CALL:
		return convertTraceCall(ethTrace, ethNetworkId)
	case web3goTypes.TRACE_CREATE:
		return convertTraceCreate(ethTrace, ethNetworkId)
	case web3goTypes.TRACE_SUICIDE:
		return convertTraceSuicide(ethTrace, ethNetworkId), nil
	// case web3goTypes.TRACE_REWARD:
	// 	return nil, nil
	default:
		return nil, nil
	}
}
