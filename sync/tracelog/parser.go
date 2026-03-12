package tracelog

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
)

// ParseEpochTraces extracts virtual logs from epoch traces for known internal contracts.
func ParseEpochTraces(epochTrace *types.EpochTrace, registry *Registry) ([]*VirtualLog, error) {
	callFrames, err := BuildCallTree(epochTrace.CfxTraces)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to build call tree")
	}

	var logs []*VirtualLog
	for _, frame := range callFrames {
		frameLogs, err := collectVirtualLogs(frame, registry)
		if err != nil {
			return nil, err
		}
		logs = append(logs, frameLogs...)
	}
	return logs, nil
}

// collectVirtualLogs walks the subtree rooted at frame, collecting virtual logs.
func collectVirtualLogs(frame *CallFrame, registry *Registry) ([]*VirtualLog, error) {
	var logs []*VirtualLog

	log, err := constructVirtualLog(frame, registry)
	if err != nil {
		return nil, err
	}
	if log != nil {
		logs = append(logs, log)
	}

	for _, child := range frame.Children {
		childLogs, err := collectVirtualLogs(child, registry)
		if err != nil {
			return nil, err
		}
		logs = append(logs, childLogs...)
	}

	return logs, nil
}

func constructVirtualLog(frame *CallFrame, registry *Registry) (*VirtualLog, error) {
	// Only process successful CALL frames that are fully successful
	if frame.Type != types.TRACE_CALL || !frame.Valid {
		return nil, nil
	}
	if frame.CallAction == nil || frame.CallResult == nil {
		return nil, errors.New("invalid call trace")
	}
	if frame.IsFullChainSuccess() {
		return nil, nil
	}

	entry, ok := registry.Lookup(frame.CallAction.To)
	if !ok {
		return nil, nil
	}

	input := frame.CallAction.Input
	if len(input) < 4 {
		return nil, nil
	}

	eventDef, ok := entry.LookupEvent(input[:4])
	if !ok {
		return nil, nil
	}

	method, err := entry.Contract.ABI.MethodById(input[:4])
	if err != nil {
		return nil, errors.WithMessage(err, "failed to find method by ID")
	}

	values, err := method.Inputs.Unpack(input[4:])
	if err != nil {
		return nil, errors.WithMessage(err, "failed to unpack method arguments")
	}

	log, err := eventDef.BuildLog(frame, values, input[4:])
	if err != nil {
		return nil, err
	}

	return &VirtualLog{
		Log:         log,
		ContractIdx: entry.ContractIdx,
		EventIdx:    eventDef.eventIndex,
	}, nil
}
