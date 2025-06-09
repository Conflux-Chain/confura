package cfxbridge

import (
	"container/list"
	"fmt"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

type stackedTraceResult struct {
	traceResult  *types.LocalizedTrace
	traceAddress []uint
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

	if err := tb.pop(nil); err != nil {
		return nil, err
	}

	return tb.traces, nil
}

func (tb *TraceBuilder) Append(trace, traceResult *types.LocalizedTrace, traceAddress []uint) error {
	// E.g. reward & suicide trace not supported in Conflux.
	if trace == nil {
		return nil
	}

	// pop previous trace result if required
	if err := tb.pop(traceAddress); err != nil {
		return err
	}

	tb.traces = append(tb.traces, *trace)

	// lazy init stack
	if tb.stackedResults == nil {
		tb.stackedResults = list.New()
	}

	// push into stack event the trace result is nil, e.g. internal_transfer_action, to keep trace address in sequence
	tb.stackedResults.PushBack(stackedTraceResult{
		traceResult:  traceResult,
		traceAddress: traceAddress,
	})

	return nil
}

func (tb *TraceBuilder) pop(traceAddress []uint) error {
	if tb.stackedResults == nil {
		return nil
	}

	for {
		// previous trace should always exist
		topEle := tb.stackedResults.Back()
		if topEle == nil {
			return fmt.Errorf("no trace adddress in stack, cur = %v", traceAddress)
		}

		pre := topEle.Value.(stackedTraceResult)
		preTraceAddressLen, curTraceAddressLen := len(pre.traceAddress), len(traceAddress)

		switch {
		// new sub trace, e.g. [x y z] => [x y z 0], do not pop any previous trace
		case curTraceAddressLen > preTraceAddressLen:
			if curTraceAddressLen > preTraceAddressLen+1 {
				return fmt.Errorf("trace address too large, pre = %v, cur = %v", pre.traceAddress, traceAddress)
			}

			if traceAddress[curTraceAddressLen-1] != 0 {
				return fmt.Errorf("sub trace starts with non-zero index, pre = %v, cur = %v", pre.traceAddress, traceAddress)
			}

			return nil

		// new sibling trace, e.g. [x y z] => [x y z+1], pop the previous trace
		case curTraceAddressLen == preTraceAddressLen:
			if preTraceAddressLen > 0 && pre.traceAddress[preTraceAddressLen-1]+1 != traceAddress[curTraceAddressLen-1] {
				return fmt.Errorf("sibling traces not in sequence, pre = %v, cur = %v", pre.traceAddress, traceAddress)
			}

			tb.traces = append(tb.traces, *pre.traceResult)
			tb.stackedResults.Remove(topEle)

			return nil

		// new uncle trace, e.g. [x y Z ...] => [x y+1], pop the entire descendant traces of last sibling trace
		default:
			tb.traces = append(tb.traces, *pre.traceResult)
			tb.stackedResults.Remove(topEle)
		}
	}
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

func (ttb *TransactionTraceBuilder) Append(trace, traceResult *types.LocalizedTrace, traceAddress []uint) (bool, error) {
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

	if err := ttb.builder.Append(trace, traceResult, traceAddress); err != nil {
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

func (btb *BlockTraceBuilder) Append(trace, traceResult *types.LocalizedTrace, traceAddress []uint) error {
	if trace == nil {
		return nil
	}

	next, err := btb.builer.Append(trace, traceResult, traceAddress)
	if err != nil || next {
		return err
	}

	if err := btb.seal(); err != nil {
		return err
	}

	_, err = btb.builer.Append(trace, traceResult, traceAddress)
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
