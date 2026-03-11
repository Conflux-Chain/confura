package tracelog

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
)

// ParseEpochTraces extracts virtual logs from epoch traces for known internal contracts.
func ParseEpochTraces(epochTrace *types.EpochTrace, registry *Registry) ([]*types.Log, error) {
	callFrames, err := BuildCallTree(epochTrace.CfxTraces)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to build call tree")
	}

	var logs []*types.Log
	for _, frame := range callFrames {
		log, err := constructVirtualLog(frame, registry)
		if err != nil {
			return nil, err
		}
		if log != nil {
			logs = append(logs, log)
		}
	}
	return logs, nil
}

func constructVirtualLog(frame *CallFrame, registry *Registry) (*types.Log, error) {
	// Only process successful CALL frames that are fully successful
	// (i.e., exclude some ancestor reverted, meaning the real event was dropped).
	if frame.Type != types.TRACE_CALL || !frame.Valid {
		return nil, nil
	}
	if frame.CallAction == nil || frame.CallResult == nil {
		return nil, nil
	}
	if frame.CallResult.Outcome != types.OUTCOME_SUCCESS {
		return nil, nil
	}
	if !frame.IsFullChainSuccess() {
		return nil, nil
	}

	// Look up the contract
	entry, ok := registry.Lookup(frame.CallAction.To)
	if !ok {
		return nil, nil
	}

	input := frame.CallAction.Input
	if len(input) < 4 {
		return nil, nil
	}

	// Look up the event definition
	eventDef, ok := entry.LookupEvent(input[:4])
	if !ok {
		return nil, nil
	}

	// Decode method arguments
	method, err := entry.Contract.ABI.MethodById(input[:4])
	if err != nil {
		return nil, errors.WithMessage(err, "failed to find method by ID")
	}

	values, err := method.Inputs.Unpack(input[4:])
	if err != nil {
		return nil, errors.WithMessage(err, "failed to unpack method arguments")
	}

	return eventDef.BuildLog(frame, values, input[4:])
}
