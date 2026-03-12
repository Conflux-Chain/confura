package tracelog

import "github.com/Conflux-Chain/go-conflux-sdk/types"

// TraceData is the common interface for all trace node payloads.
type TraceData interface {
	IsSuccess() bool
	TraceType() types.TraceType
	Trace() *types.LocalizedTrace
}

type BaseTraceData struct {
	*types.LocalizedTrace
}

func (d *BaseTraceData) TraceType() types.TraceType {
	return d.Type
}

func (d *BaseTraceData) Trace() *types.LocalizedTrace {
	return d.LocalizedTrace
}

type CallTraceData struct {
	*BaseTraceData
	Action types.Call
	Result types.CallResult
}

func (d *CallTraceData) IsSuccess() bool {
	return d.Result.Outcome == types.OUTCOME_SUCCESS
}

type CreateTraceData struct {
	*BaseTraceData
	Action types.Create
	Result types.CreateResult
}

func (d *CreateTraceData) IsSuccess() bool {
	return d.Result.Outcome == types.OUTCOME_SUCCESS
}
