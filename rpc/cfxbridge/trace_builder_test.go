package cfxbridge

import (
	"fmt"
	"testing"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/stretchr/testify/assert"
)

func (tb *TraceBuilder) mustAppendTest(t *testing.T, name string, result bool, traceAddress []uint) {
	var trace *types.LocalizedTrace
	if len(name) > 0 {
		trace = &types.LocalizedTrace{
			Action: name,
		}
	}

	var traceResult *types.LocalizedTrace
	if len(name) > 0 && result {
		traceResult = &types.LocalizedTrace{
			Action: name + "'",
		}
	}

	err := tb.Append(trace, traceResult, traceAddress)
	assert.NoError(t, err)
}

func (tb *TraceBuilder) mustBuild(t *testing.T, names ...string) {
	traces, err := tb.Build()
	assert.NoError(t, err)

	var traceNames []string
	for _, v := range traces {
		traceNames = append(traceNames, v.Action.(string))
	}

	assert.Equal(t, names, traceNames)
}

func (tb *TraceBuilder) debugTest() {
	var traces []string
	for _, v := range tb.traces {
		traces = append(traces, v.Action.(string))
	}

	var stacked []string
	if tb.stackedResults != nil && tb.stackedResults.Len() > 0 {
		for cur := tb.stackedResults.Front(); cur != nil; cur = cur.Next() {
			stacked = append(stacked, cur.Value.(stackedTraceResult).traceResult.Action.(string))
		}
	}

	fmt.Printf("================= Builder: traces = %v, stacked = %v\n", traces, stacked)
}

func TestTraceBuilderOnlyRoot(t *testing.T) {
	var builder TraceBuilder

	builder.mustAppendTest(t, "R", true, []uint{})

	builder.mustBuild(t, "R", "R'")
}

func TestTraceBuilderOnlyRootNil(t *testing.T) {
	var builder TraceBuilder

	builder.mustAppendTest(t, "R", true, nil)

	builder.mustBuild(t, "R", "R'")
}

func TestTraceBuilderEmbedded(t *testing.T) {
	var builder TraceBuilder

	// root
	builder.mustAppendTest(t, "R", true, []uint{})
	builder.debugTest()

	// root - 0
	builder.mustAppendTest(t, "R0", true, []uint{0})
	builder.mustAppendTest(t, "R00", true, []uint{0, 0})
	builder.mustAppendTest(t, "R01", false, []uint{0, 1}) // suicide trace without result R01'
	builder.mustAppendTest(t, "R02", true, []uint{0, 2})

	// root - 1
	builder.mustAppendTest(t, "R1", true, []uint{1})
	builder.mustAppendTest(t, "R10", true, []uint{1, 0})
	builder.mustAppendTest(t, "R11", true, []uint{1, 1})
	builder.mustAppendTest(t, "R110", true, []uint{1, 1, 0})
	builder.mustAppendTest(t, "R111", true, []uint{1, 1, 1})

	// root - 2
	builder.mustAppendTest(t, "R2", true, []uint{2})
	builder.mustAppendTest(t, "R20", true, []uint{2, 0})
	builder.mustAppendTest(t, "R200", true, []uint{2, 0, 0})

	// build
	builder.mustBuild(t,
		"R",
		"R0", "R00", "R00'", "R01", "R02", "R02'", "R0'",
		"R1", "R10", "R10'", "R11", "R110", "R110'", "R111", "R111'", "R11'", "R1'",
		"R2", "R20", "R200", "R200'", "R20'", "R2'",
		"R'",
	)
}
