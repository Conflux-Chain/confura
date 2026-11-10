package cfxbridge

import (
	"fmt"
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

func convertTraceCall(ethTrace *web3goTypes.LocalizedTrace, ethNetworkId uint32) (*types.LocalizedTrace, *types.LocalizedTrace, error) {
	ethActionCall, ok := ethTrace.Action.(web3goTypes.Call)
	if !ok {
		return nil, nil, fmt.Errorf("invalid upstream call action %T", ethTrace.Action)
	}
	ethCallResult, ok := ethTrace.Result.(web3goTypes.CallResult)
	if !ok && (ethTrace.Result != nil || ethTrace.Error == nil) {
		return nil, nil, fmt.Errorf("incomplete upstream call trace: invalid result %T", ethTrace.Result)
	}
	gasLeft, err := traceGasLeft(ethActionCall.Gas, ethActionCall.Value, ethCallResult.GasUsed, ethTrace.Error != nil)
	if err != nil {
		return nil, nil, fmt.Errorf("incomplete upstream call trace: %w", err)
	}
	cfxTraceCall := convertTrace(ethTrace, types.TRACE_CALL, types.Call{
		Space:    types.SPACE_EVM,
		From:     ConvertAddress(ethActionCall.From, ethNetworkId),
		To:       ConvertAddress(ethActionCall.To, ethNetworkId),
		Value:    hexutil.Big(*ethActionCall.Value),
		Gas:      hexutil.Big(*ethActionCall.Gas),
		Input:    ethActionCall.Input,
		CallType: types.CallType(ethActionCall.CallType),
	})

	cfxTraceCallResult := convertTrace(ethTrace, types.TRACE_CALL_RESULT, types.CallResult{
		Outcome:    convertTraceOutcome(ethTrace),
		GasLeft:    hexutil.Big(*gasLeft),
		ReturnData: ethCallResult.Output,
	})

	return cfxTraceCall, cfxTraceCallResult, nil
}

func convertTraceCreate(ethTrace *web3goTypes.LocalizedTrace, ethNetworkId uint32) (*types.LocalizedTrace, *types.LocalizedTrace, error) {
	ethActionCreate, ok := ethTrace.Action.(web3goTypes.Create)
	if !ok {
		return nil, nil, fmt.Errorf("invalid upstream create action %T", ethTrace.Action)
	}
	ethCreateResult, ok := ethTrace.Result.(web3goTypes.CreateResult)
	if !ok && (ethTrace.Result != nil || ethTrace.Error == nil) {
		return nil, nil, fmt.Errorf("incomplete upstream create trace: invalid result %T", ethTrace.Result)
	}
	gasLeft, err := traceGasLeft(ethActionCreate.Gas, ethActionCreate.Value, ethCreateResult.GasUsed, ethTrace.Error != nil)
	if err != nil {
		return nil, nil, fmt.Errorf("incomplete upstream create trace: %w", err)
	}
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

	cfxTraceCreateResult := convertTrace(ethTrace, types.TRACE_CREATE_RESULT, types.CreateResult{
		Outcome:    convertTraceOutcome(ethTrace),
		Addr:       ConvertAddress(ethCreateResult.Address, ethNetworkId),
		GasLeft:    hexutil.Big(*gasLeft),
		ReturnData: ethCreateResult.Code,
	})

	return cfxTraceCreate, cfxTraceCreateResult, nil
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

// ConvertTrace preserves the bridge mapping and reports malformed traces without
// panicking. Failed traces may omit their result; see traceGasLeft.
func ConvertTrace(ethTrace *web3goTypes.LocalizedTrace, ethNetworkId uint32) (*types.LocalizedTrace, *types.LocalizedTrace, error) {
	if ethTrace == nil || ethTrace.Type == web3goTypes.TRACE_REWARD {
		return nil, nil, nil
	}
	switch ethTrace.Type {
	case web3goTypes.TRACE_CALL, web3goTypes.TRACE_CREATE, web3goTypes.TRACE_SUICIDE:
	default:
		return nil, nil, nil
	}
	if ethTrace.TransactionHash == nil {
		return nil, nil, fmt.Errorf("incomplete upstream trace: missing transaction hash")
	}

	switch ethTrace.Type {
	case web3goTypes.TRACE_CALL:
		return convertTraceCall(ethTrace, ethNetworkId)
	case web3goTypes.TRACE_CREATE:
		return convertTraceCreate(ethTrace, ethNetworkId)
	case web3goTypes.TRACE_SUICIDE:
		action, ok := ethTrace.Action.(web3goTypes.Suicide)
		if !ok || action.Balance == nil {
			return nil, nil, fmt.Errorf("incomplete upstream suicide trace: invalid action or balance")
		}
		return convertTraceSuicide(ethTrace, ethNetworkId), nil, nil
	default:
		return nil, nil, nil
	}
}

func traceGasLeft(gas, value, used *big.Int, failed bool) (*big.Int, error) {
	if gas == nil || value == nil {
		return nil, fmt.Errorf("missing action gas or value")
	}
	if used == nil {
		if !failed {
			return nil, fmt.Errorf("missing result gasUsed")
		}
		// Conflux eSpace omits failed results. Zero is a compatibility value,
		// not measured gas; output/code and failed create address stay empty.
		return new(big.Int), nil
	}
	return new(big.Int).Sub(gas, used), nil
}
