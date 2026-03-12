package tracelog

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
)

// VirtualLog wraps a types.Log with its compact index identifiers.
type VirtualLog struct {
	*types.Log
	ContractIdx ContractIndex
	EventIdx    EventIndex
}

// ParseEpochTraces extracts virtual logs from epoch traces.
func ParseEpochTraces(epochTrace *types.EpochTrace, registry *Registry) ([]*VirtualLog, error) {
	roots, err := BuildCallTree(epochTrace.CfxTraces)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to build call tree")
	}

	var vlogs []*VirtualLog
	err = Walk(roots, func(frame *CallFrame) error {
		log, err := constructVirtualLog(frame, registry)
		if err != nil {
			return err
		}
		if log != nil {
			vlogs = append(vlogs, log)
		}
		return nil
	})

	return vlogs, err
}

func constructVirtualLog(frame *CallFrame, registry *Registry) (*VirtualLog, error) {
	if !frame.fullChainSuccess {
		return nil, nil
	}

	trace := frame.Data.Trace()
	if trace.Type != types.TRACE_CALL || !trace.Valid {
		return nil, nil
	}

	callData, ok := frame.Data.(*CallTraceData)
	if !ok {
		return nil, errors.New("invalid call trace data")
	}

	entry, ok := registry.Lookup(callData.Action.To)
	if !ok {
		return nil, nil
	}

	input := callData.Action.Input
	if len(input) < 4 {
		return nil, nil
	}

	eventDef, ok := entry.LookupEvent(input[:4])
	if !ok {
		return nil, nil
	}

	method, err := entry.Contract.ABI.MethodById(input[:4])
	if err != nil {
		return nil, errors.WithMessage(err, "method not found")
	}

	values, err := method.Inputs.Unpack(input[4:])
	if err != nil {
		return nil, errors.WithMessage(err, "unpack failed")
	}

	log, err := eventDef.BuildLog(callData, values, input[4:])
	if err != nil {
		return nil, err
	}

	return &VirtualLog{
		Log:         log,
		ContractIdx: entry.ContractIdx,
		EventIdx:    eventDef.eventIndex,
	}, nil
}
