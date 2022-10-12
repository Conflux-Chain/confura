package cfxbridge

import (
	"container/list"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/sirupsen/logrus"
)

type stackedTraceResult struct {
	traceResult *types.LocalizedTrace
	subTraces   uint
}

// TraceBuilder builds traces in stack way and thread unsafe.
type TraceBuilder struct {
	traces         []types.LocalizedTrace
	stackedResults *list.List
}

func (tb *TraceBuilder) Build() []types.LocalizedTrace {
	if tb.traces == nil {
		return emptyTraces
	}

	if tb.stackedResults != nil && tb.stackedResults.Len() != 0 {
		var leftover []*stackedTraceResult

		// This shouldn't happen if push/pop operation pairs correctly, anyway
		// let's append the leftover result traces for completeness.
		for v := tb.stackedResults.Back(); v != nil; {
			tr := v.Value.(*stackedTraceResult)
			tb.traces = append(tb.traces, *tr.traceResult)

			nv := v.Prev()
			tb.stackedResults.Remove(v)
			v = nv

			leftover = append(leftover, tr)
		}

		logrus.WithField("leftOverResultTraces", leftover).Warn("Mismatched result trace stack operations")
	}

	return tb.traces
}

func (tb *TraceBuilder) Append(trace, traceResult *types.LocalizedTrace, subTraces uint) {
	// E.g. reward & suicide trace not supported in Conflux.
	if trace == nil {
		return
	}

	tb.traces = append(tb.traces, *trace)

	if subTraces == 0 {
		if traceResult != nil {
			tb.traces = append(tb.traces, *traceResult)
		}

		tb.pop()
	} else if traceResult != nil {
		tb.push(traceResult, subTraces)
	}
}

func (tb *TraceBuilder) push(traceResult *types.LocalizedTrace, subTraces uint) {
	// Lazy initialize the stack, but thread unsafe.
	if tb.stackedResults == nil {
		tb.stackedResults = list.New()
	}

	tb.stackedResults.PushBack(&stackedTraceResult{
		traceResult: traceResult,
		subTraces:   subTraces,
	})
}

func (tb *TraceBuilder) pop() {
	// No item pushed into stack before
	if tb.stackedResults == nil {
		return
	}

	// No pending trace result to handle
	topEle := tb.stackedResults.Back()
	if topEle == nil {
		return
	}

	item := topEle.Value.(*stackedTraceResult)

	// Should never happen, but make code robust
	if item.subTraces == 0 {
		logrus.WithField("tx", item.traceResult.TransactionHash.String()).Error("Failed to pop due to invalid subtraces")
		return
	}

	item.subTraces--

	// There are remaining sub traces that unhandled
	if item.subTraces > 0 {
		return
	}

	// All sub traces handled and pop the trace result
	tb.traces = append(tb.traces, *item.traceResult)
	tb.stackedResults.Remove(topEle)

	// Pop upstream trace
	tb.pop()
}

type TransactionTraceBuilder struct {
	txTrace types.LocalizedTransactionTrace
	builder TraceBuilder
}

func (ttb *TransactionTraceBuilder) Build() (*types.LocalizedTransactionTrace, bool) {
	if len(ttb.txTrace.TransactionHash) == 0 {
		return nil, false
	}

	ttb.txTrace.Traces = ttb.builder.Build()
	return &ttb.txTrace, true
}

func (ttb *TransactionTraceBuilder) Append(trace, traceResult *types.LocalizedTrace, subTraces uint) bool {
	if trace == nil {
		// ignore nil trace and continue to append other traces
		return true
	}

	if len(ttb.txTrace.TransactionHash) == 0 {
		// initialize transaction hash and position with the first trace.
		ttb.txTrace.TransactionHash = *trace.TransactionHash
		if trace.TransactionPosition != nil {
			ttb.txTrace.TransactionPosition = *trace.TransactionPosition
		}
	} else if ttb.txTrace.TransactionHash != *trace.TransactionHash { // next new transaction
		return false
	}

	ttb.builder.Append(trace, traceResult, subTraces)

	return true
}

type BlockTraceBuilder struct {
	txTraces []types.LocalizedTransactionTrace
	builer   TransactionTraceBuilder
}

func (btb *BlockTraceBuilder) Build() []types.LocalizedTransactionTrace {
	btb.seal()

	if btb.txTraces == nil {
		return emptyTxTraces
	}

	return btb.txTraces
}

func (btb *BlockTraceBuilder) Append(trace, traceResult *types.LocalizedTrace, subTraces uint) {
	if trace == nil {
		return
	}

	if btb.builer.Append(trace, traceResult, subTraces) {
		return
	}

	btb.seal()
	btb.builer.Append(trace, traceResult, subTraces)
}

func (btb *BlockTraceBuilder) seal() {
	if txTrace, ok := btb.builer.Build(); ok {
		btb.txTraces = append(btb.txTraces, *txTrace)
		btb.builer = TransactionTraceBuilder{}
	}
}
