package cfxbridge

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3goTypes "github.com/openweb3/web3go/types"
)

var (
	cfxTraceErrorReverted = "Reverted"
	cfxTraceGasLeftZero   = *types.NewBigInt(0)
	cfxTraceBytesEmpty    = []byte{}

	cfxTraceCallResultReverted = types.CallResult{
		Outcome:    types.OUTCOME_REVERTED,
		GasLeft:    cfxTraceGasLeftZero,
		ReturnData: cfxTraceBytesEmpty,
	}
)

func convertTrace(ethTrace *web3goTypes.LocalizedTrace, cfxTraceType types.TraceType, cfxTraceAction any) *types.LocalizedTrace {
	// transaction position
	var txPos *hexutil.Uint64
	if ethTrace.TransactionPosition != nil {
		txPosU64 := hexutil.Uint64(*ethTrace.TransactionPosition)
		txPos = &txPosU64
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
		EpochHash:           ConvertHashNullable(&ethTrace.BlockHash),
		EpochNumber:         types.NewBigInt(ethTrace.BlockNumber),
		BlockHash:           ConvertHashNullable(&ethTrace.BlockHash),
		TransactionPosition: txPos,
		TransactionHash:     ConvertHashNullable(ethTrace.TransactionHash),
	}
}

func convertTraceCall(ethTrace *web3goTypes.LocalizedTrace, ethNetworkId uint32) (*types.LocalizedTrace, *types.LocalizedTrace) {
	ethActionCall := ethTrace.Action.(web3goTypes.Call)
	cfxTraceCall := convertTrace(ethTrace, types.TRACE_CALL, types.Call{
		Space:    types.SPACE_EVM,
		From:     ConvertAddress(ethActionCall.From, ethNetworkId),
		To:       ConvertAddress(ethActionCall.To, ethNetworkId),
		Value:    *types.NewBigIntByRaw(ethActionCall.Value),
		Gas:      *types.NewBigIntByRaw(ethActionCall.Gas),
		Input:    ethActionCall.Input,
		CallType: types.CallType(ethActionCall.CallType),
	})

	var cfxCallResult any
	if ethTrace.Error == nil {
		ethCallResult := ethTrace.Result.(web3goTypes.CallResult)
		cfxCallResult = types.CallResult{
			Outcome:    types.OUTCOME_SUCCESS,
			GasLeft:    *types.NewBigIntByRaw(ethCallResult.GasUsed),
			ReturnData: ethCallResult.Output,
		}
	} else if *ethTrace.Error == cfxTraceErrorReverted {
		cfxCallResult = cfxTraceCallResultReverted
	} else {
		cfxCallResult = types.CallResult{
			Outcome:    types.OUTCOME_FAIL,
			GasLeft:    cfxTraceGasLeftZero,
			ReturnData: []byte(*ethTrace.Error),
		}
	}

	cfxTraceCallResult := convertTrace(ethTrace, types.TRACE_CALL_RESULT, cfxCallResult)

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
		Value:      *types.NewBigIntByRaw(ethActionCreate.Value),
		Gas:        *types.NewBigIntByRaw(ethActionCreate.Gas),
		Init:       ethActionCreate.Init,
		CreateType: createType,
	})

	var cfxCreateResult any
	if ethTrace.Error == nil {
		ethCreateResult := ethTrace.Result.(web3goTypes.CreateResult)
		cfxCreateResult = types.CreateResult{
			Outcome:    types.OUTCOME_SUCCESS,
			Addr:       ConvertAddress(ethCreateResult.Address, ethNetworkId),
			GasLeft:    *types.NewBigIntByRaw(ethCreateResult.GasUsed),
			ReturnData: ethCreateResult.Code,
		}
	} else if *ethTrace.Error == cfxTraceErrorReverted {
		cfxCreateResult = types.CreateResult{
			Outcome:    types.OUTCOME_REVERTED,
			Addr:       ConvertAddress(common.Address{}, ethNetworkId),
			GasLeft:    cfxTraceGasLeftZero,
			ReturnData: cfxTraceBytesEmpty,
		}
	} else {
		cfxCreateResult = types.CreateResult{
			Outcome:    types.OUTCOME_FAIL,
			Addr:       ConvertAddress(common.Address{}, ethNetworkId),
			GasLeft:    cfxTraceGasLeftZero,
			ReturnData: []byte(*ethTrace.Error),
		}
	}

	cfxTraceCreateResult := convertTrace(ethTrace, types.TRACE_CREATE_RESULT, cfxCreateResult)

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
