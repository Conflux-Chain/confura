package cfxbridge

import (
	"container/list"
	"errors"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
)

var (
	errWrongTraceStack = errors.New("wrong trace stack")
)

type stackedTraceResult struct {
	traceResult *types.LocalizedTrace
	subTraces   hexutil.Uint64
}

// TraceBuilder builds traces in stack way and thread unsafe.
type TraceBuilder struct {
	traces         []types.LocalizedTrace
	stackedResults *list.List
}

func (tb *TraceBuilder) Build() ([]types.LocalizedTrace, error) {
	if tb.traces == nil {
		return emptyTraces, nil
	}

	// This shouldn't happen if stack push/pop operation pairs correctly.
	if tb.stackedResults != nil && tb.stackedResults.Len() != 0 {
		logrus.WithFields(logrus.Fields{
			"stackLen":  tb.stackedResults.Len(),
			"numTraces": len(tb.traces),
		}).Error("Mismatched push/pop operation pairs for trace result stack")
		return nil, errWrongTraceStack
	}

	return tb.traces, nil
}

func (tb *TraceBuilder) Append(trace, traceResult *types.LocalizedTrace, subTraces hexutil.Uint64) error {
	// E.g. reward & suicide trace not supported in Conflux.
	if trace == nil {
		return nil
	}

	tb.traces = append(tb.traces, *trace)

	// If there are any subtraces for this one, just push the trace result
	// to stack for later retrieval.
	if subTraces != 0 {
		if traceResult != nil {
			tb.push(traceResult, subTraces)
		}
		return nil
	}

	// Otherwise, this trace is the end one of call stack. Need to append the
	// trace result and pop the stack.
	if traceResult != nil {
		tb.traces = append(tb.traces, *traceResult)
	}

	return tb.pop()
}

func (tb *TraceBuilder) push(traceResult *types.LocalizedTrace, subTraces hexutil.Uint64) {
	// Lazy initialize the stack, but thread unsafe.
	if tb.stackedResults == nil {
		tb.stackedResults = list.New()
	}

	tb.stackedResults.PushBack(&stackedTraceResult{
		traceResult: traceResult,
		subTraces:   subTraces,
	})
}

func (tb *TraceBuilder) pop() error {
	// No item pushed into stack before
	if tb.stackedResults == nil {
		return nil
	}

	// No pending trace result to handle
	topEle := tb.stackedResults.Back()
	if topEle == nil {
		return nil
	}

	item := topEle.Value.(*stackedTraceResult)

	// Should never happen, but make code robust
	if item.subTraces == 0 {
		logrus.WithFields(logrus.Fields{
			"txnHash":   item.traceResult.TransactionHash.String(),
			"traceType": item.traceResult.Type,
		}).Error("Failed to pop due to invalid subtraces")
		return errWrongTraceStack
	}

	item.subTraces--

	// There are remaining sub traces that unhandled
	if item.subTraces > 0 {
		return nil
	}

	// All sub traces handled and pop the trace result
	tb.traces = append(tb.traces, *item.traceResult)
	tb.stackedResults.Remove(topEle)

	// Pop upstream trace
	return tb.pop()
}

type TransactionTraceBuilder struct {
	txTrace types.LocalizedTransactionTrace
	builder TraceBuilder
}

func (ttb *TransactionTraceBuilder) Build() (*types.LocalizedTransactionTrace, bool, error) {
	if len(ttb.txTrace.TransactionHash) == 0 {
		return nil, false, nil
	}

	traces, err := ttb.builder.Build()
	if err != nil {
		return nil, false, err
	}

	ttb.txTrace.Traces = traces
	return &ttb.txTrace, true, nil
}

func (ttb *TransactionTraceBuilder) Append(trace, traceResult *types.LocalizedTrace, subTraces hexutil.Uint64) (bool, error) {
	if trace == nil {
		// ignore nil trace and continue to append other traces
		return true, nil
	}

	if len(ttb.txTrace.TransactionHash) == 0 {
		// initialize transaction hash and position with the first trace.
		ttb.txTrace.TransactionHash = *trace.TransactionHash
		if trace.TransactionPosition != nil {
			ttb.txTrace.TransactionPosition = *trace.TransactionPosition
		}
	} else if ttb.txTrace.TransactionHash != *trace.TransactionHash { // next new transaction
		return false, nil
	}

	if err := ttb.builder.Append(trace, traceResult, subTraces); err != nil {
		return false, err
	}

	return true, nil
}

type BlockTraceBuilder struct {
	txTraces []types.LocalizedTransactionTrace
	builer   TransactionTraceBuilder
}

func (btb *BlockTraceBuilder) Build() ([]types.LocalizedTransactionTrace, error) {
	if err := btb.seal(); err != nil {
		return nil, err
	}

	if btb.txTraces == nil {
		return emptyTxTraces, nil
	}

	return btb.txTraces, nil
}

func (btb *BlockTraceBuilder) Append(trace, traceResult *types.LocalizedTrace, subTraces hexutil.Uint64) error {
	if trace == nil {
		return nil
	}

	next, err := btb.builer.Append(trace, traceResult, subTraces)
	if err != nil || next {
		return err
	}

	if err := btb.seal(); err != nil {
		return err
	}

	_, err = btb.builer.Append(trace, traceResult, subTraces)
	return err
}

func (btb *BlockTraceBuilder) seal() error {
	txTrace, ok, err := btb.builer.Build()
	if err != nil {
		return err
	}

	if ok {
		btb.txTraces = append(btb.txTraces, *txTrace)
		btb.builer = TransactionTraceBuilder{}
	}

	return nil
}
