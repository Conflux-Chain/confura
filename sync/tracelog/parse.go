package tracelog

import (
	"encoding/hex"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// VirtualLog wraps a types.Log with its compact index identifiers.
type VirtualLog struct {
	*types.Log
	ContractIdx uint8
	EventIdx    uint8
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
		logrus.WithFields(logrus.Fields{
			"callAction.To": callData.Action.To,
		}).Debug("Skip un-registered internal contract")
		return nil, nil
	}

	input := callData.Action.Input
	if len(input) < 4 {
		return nil, errors.Errorf("expected at least 4 bytes of call input got %v", len(input))
	}

	eventDef, ok := entry.LookupEvent(input[:4])
	if !ok {
		logrus.WithFields(logrus.Fields{
			"callAction.To": callData.Action.To,
			"methodID":      hex.EncodeToString(input[:4]),
		}).Debug("Skip un-registered event")
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
		logrus.WithFields(logrus.Fields{
			"traceEpoch":    trace.EpochNumber.ToInt().Uint64(),
			"contract":      entry.Contract.Address,
			"callAction.To": callData.Action.To,
			"methodID":      method.String(),
			"values":        values,
		}).WithError(err).Info("Failed to construct virtual log from internal contract event definition")
		return nil, errors.WithMessage(err, "failed to build log")
	}

	return &VirtualLog{
		Log:         log,
		ContractIdx: entry.ContractIdx,
		EventIdx:    eventDef.eventIndex,
	}, nil
}
