package tracelog

import (
	"math/big"
	"strings"
	"testing"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Fixtures helpers
// ---------------------------------------------------------------------------
func mustAddr(hex string) types.Address {
	return cfxaddress.MustNew(hex, 1)
}

func hexBig(v int64) hexutil.Big {
	return hexutil.Big(*big.NewInt(v))
}

// baseLocalized returns a LocalizedTrace with all common fields pre-filled.
// Individual test cases may override fields as needed.
func baseLocalized(traceType types.TraceType, action interface{}) *types.LocalizedTrace {
	return &types.LocalizedTrace{
		Type:                traceType,
		Action:              action,
		Valid:               true,
		EpochHash:           types.Hash("0xec4fc7f29ef5f3367be2bfef3718b3e167416e811d7c2707c58e174bbb3a900c"),
		EpochNumber:         hexBig(121630605),
		BlockHash:           types.Hash("0xec4fc7f29ef5f3367be2bfef3718b3e167416e811d7c2707c58e174bbb3a900c"),
		TransactionPosition: hexutil.Uint64(0),
		TransactionHash:     types.Hash("0x89b14531507aa7f680ace24d2a4c8f42387e03ac63bb2d6331d42a1d714ed1d1"),
	}
}

func callTrace() *types.LocalizedTrace {
	return baseLocalized(types.TRACE_CALL, types.Call{
		Space:    types.SPACE_NATIVE,
		From:     mustAddr("0x1000000000000000000000000000000000000001"),
		To:       mustAddr("0x2000000000000000000000000000000000000002"),
		Gas:      hexBig(100000),
		CallType: types.CALL_CALL,
	})
}

func callResultTrace(outcome types.OutcomeType) *types.LocalizedTrace {
	return baseLocalized(types.TRACE_CALL_RESULT, types.CallResult{
		Outcome: outcome,
		GasLeft: hexBig(1666),
	})
}

func createTrace() *types.LocalizedTrace {
	return baseLocalized(types.TRACE_CREATE, types.Create{
		Space:      types.SPACE_NATIVE,
		From:       mustAddr("0x1000000000000000000000000000000000000001"),
		Gas:        hexBig(200000),
		CreateType: types.CREATE_CREATE,
	})
}

func createResultTrace(outcome types.OutcomeType) *types.LocalizedTrace {
	return baseLocalized(types.TRACE_CREATE_RESULT, types.CreateResult{
		Outcome: outcome,
		GasLeft: hexBig(50000),
	})
}

func internalTransferTrace() *types.LocalizedTrace {
	return baseLocalized(types.TRACE_INTERNAL_TRANSFER_ACTIION, types.InternalTransferAction{
		From:       mustAddr("0x1000000000000000000000000000000000000001"),
		FromPocket: types.POCKET_BALANCE,
		FromSpace:  types.SPACE_NATIVE,
		To:         mustAddr("0x2000000000000000000000000000000000000002"),
		ToPocket:   types.POCKET_STAKING_BALANCE,
		ToSpace:    types.SPACE_NATIVE,
		Value:      hexBig(1000),
	})
}

// withMutation returns a shallow copy of t with the given field overridden via a
// mutator function – keeps individual test cases concise.
func withMutation(t *types.LocalizedTrace, mutate func(*types.LocalizedTrace)) *types.LocalizedTrace {
	copy := *t
	mutate(&copy)
	return &copy
}

// ---------------------------------------------------------------------------
// TC-01  Single Call – success
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC01_SingleCallSuccess(t *testing.T) {
	traces := []*types.LocalizedTrace{
		internalTransferTrace(), // should be skipped
		callTrace(),
		internalTransferTrace(), // child internal transfer – skipped
		callResultTrace(types.OUTCOME_SUCCESS),
		internalTransferTrace(), // should be skipped
	}

	roots, err := BuildCallTree(traces)
	require.NoError(t, err)
	require.Len(t, roots, 1)

	root := roots[0]
	data, ok := root.Data.(*CallTraceData)
	require.True(t, ok)
	assert.Equal(t, types.OUTCOME_SUCCESS, data.Result.Outcome)
	assert.True(t, root.fullChainSuccess)
	assert.Empty(t, root.Children)
	assert.Nil(t, root.Parent)
}

// ---------------------------------------------------------------------------
// TC-02  Single Call – revert
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC02_SingleCallRevert(t *testing.T) {
	traces := []*types.LocalizedTrace{
		callTrace(),
		callResultTrace(types.OUTCOME_REVERTED),
	}

	roots, err := BuildCallTree(traces)
	require.NoError(t, err)
	require.Len(t, roots, 1)

	data := roots[0].Data.(*CallTraceData)
	assert.Equal(t, types.OUTCOME_REVERTED, data.Result.Outcome)
	assert.False(t, roots[0].fullChainSuccess)
}

// ---------------------------------------------------------------------------
// TC-03  Nested Call – parent success, child success
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC03_NestedBothSuccess(t *testing.T) {
	traces := []*types.LocalizedTrace{
		callTrace(),                            // A
		callTrace(),                            // B (child of A)
		callResultTrace(types.OUTCOME_SUCCESS), // result of B
		callResultTrace(types.OUTCOME_SUCCESS), // result of A
	}

	roots, err := BuildCallTree(traces)
	require.NoError(t, err)
	require.Len(t, roots, 1)

	a := roots[0]
	require.Len(t, a.Children, 1)
	b := a.Children[0]

	assert.True(t, a.fullChainSuccess)
	assert.True(t, b.fullChainSuccess)
	assert.Equal(t, a, b.Parent)
}

// ---------------------------------------------------------------------------
// TC-04  Nested Call – parent success, child fail
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC04_ParentSuccessChildFail(t *testing.T) {
	traces := []*types.LocalizedTrace{
		callTrace(),
		callTrace(),
		callResultTrace(types.OUTCOME_REVERTED), // child fails
		callResultTrace(types.OUTCOME_SUCCESS),  // parent succeeds
	}

	roots, err := BuildCallTree(traces)
	require.NoError(t, err)

	a := roots[0]
	b := a.Children[0]

	assert.True(t, a.fullChainSuccess)
	assert.False(t, b.fullChainSuccess) // child itself failed
}

// ---------------------------------------------------------------------------
// TC-05  Nested Call – parent fail, child success → child inherits false
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC05_ParentFailChildSuccess(t *testing.T) {
	traces := []*types.LocalizedTrace{
		callTrace(),
		callTrace(),
		callResultTrace(types.OUTCOME_SUCCESS),  // child succeeds
		callResultTrace(types.OUTCOME_REVERTED), // parent fails
	}

	roots, err := BuildCallTree(traces)
	require.NoError(t, err)

	a := roots[0]
	b := a.Children[0]

	assert.False(t, a.fullChainSuccess)
	assert.False(t, b.fullChainSuccess) // parent chain broken
}

// ---------------------------------------------------------------------------
// TC-06  Create – success
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC06_CreateSuccess(t *testing.T) {
	traces := []*types.LocalizedTrace{
		createTrace(),
		createResultTrace(types.OUTCOME_SUCCESS),
	}

	roots, err := BuildCallTree(traces)
	require.NoError(t, err)
	require.Len(t, roots, 1)

	data, ok := roots[0].Data.(*CreateTraceData)
	require.True(t, ok)
	assert.Equal(t, types.OUTCOME_SUCCESS, data.Result.Outcome)
	assert.True(t, roots[0].fullChainSuccess)
}

// ---------------------------------------------------------------------------
// TC-07  Multiple root nodes
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC07_MultipleRoots(t *testing.T) {
	traces := []*types.LocalizedTrace{
		callTrace(),
		callResultTrace(types.OUTCOME_SUCCESS),
		callTrace(),
		callResultTrace(types.OUTCOME_SUCCESS),
	}

	roots, err := BuildCallTree(traces)
	require.NoError(t, err)
	require.Len(t, roots, 2)

	assert.True(t, roots[0].fullChainSuccess)
	assert.True(t, roots[1].fullChainSuccess)
	assert.Nil(t, roots[0].Parent)
	assert.Nil(t, roots[1].Parent)
}

// ---------------------------------------------------------------------------
// TC-08  Empty trace list
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC08_EmptyInput(t *testing.T) {
	roots, err := BuildCallTree(nil)
	require.NoError(t, err)
	assert.Empty(t, roots)
}

// ---------------------------------------------------------------------------
// TC-09  Only internal_transfer_action traces – all skipped
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC09_OnlyInternalTransfers(t *testing.T) {
	traces := []*types.LocalizedTrace{
		internalTransferTrace(),
		internalTransferTrace(),
	}

	roots, err := BuildCallTree(traces)
	require.NoError(t, err)
	assert.Empty(t, roots)
}

// ---------------------------------------------------------------------------
// TC-10  Extra call_result with empty stack → error
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC10_ExtraResultEmptyStack(t *testing.T) {
	traces := []*types.LocalizedTrace{
		callResultTrace(types.OUTCOME_SUCCESS), // no matching call
	}

	_, err := BuildCallTree(traces)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pop empty stack")
}

// ---------------------------------------------------------------------------
// TC-11  Call without result → stack non-empty at Build
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC11_CallWithoutResult(t *testing.T) {
	traces := []*types.LocalizedTrace{
		callTrace(), // no call_result follows
	}

	_, err := BuildCallTree(traces)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmatched frames remaining")
}

// ---------------------------------------------------------------------------
// TC-12  Call receives create_result → type mismatch error
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC12_CallReceivesCreateResult(t *testing.T) {
	traces := []*types.LocalizedTrace{
		callTrace(),
		createResultTrace(types.OUTCOME_SUCCESS), // wrong result type
	}

	_, err := BuildCallTree(traces)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected call result")
}

// ---------------------------------------------------------------------------
// TC-13  Create receives call_result → type mismatch error
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC13_CreateReceivesCallResult(t *testing.T) {
	traces := []*types.LocalizedTrace{
		createTrace(),
		callResultTrace(types.OUTCOME_SUCCESS), // wrong result type
	}

	_, err := BuildCallTree(traces)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected create result")
}

// ---------------------------------------------------------------------------
// TC-14  BlockHash mismatch between action and result
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC14_BlockHashMismatch(t *testing.T) {
	result := withMutation(callResultTrace(types.OUTCOME_SUCCESS), func(t *types.LocalizedTrace) {
		t.BlockHash = types.Hash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	})

	traces := []*types.LocalizedTrace{callTrace(), result}
	_, err := BuildCallTree(traces)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmatched trace action and result")
}

// ---------------------------------------------------------------------------
// TC-15  TransactionHash mismatch between action and result
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC15_TransactionHashMismatch(t *testing.T) {
	result := withMutation(callResultTrace(types.OUTCOME_SUCCESS), func(t *types.LocalizedTrace) {
		t.TransactionHash = types.Hash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	})

	traces := []*types.LocalizedTrace{callTrace(), result}
	_, err := BuildCallTree(traces)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmatched trace action and result")
}

// ---------------------------------------------------------------------------
// TC-16  EpochNumber mismatch between action and result
// ---------------------------------------------------------------------------

func TestBuildCallTree_TC16_EpochNumberMismatch(t *testing.T) {
	result := withMutation(callResultTrace(types.OUTCOME_SUCCESS), func(t *types.LocalizedTrace) {
		t.EpochNumber = hexBig(999999)
	})

	traces := []*types.LocalizedTrace{callTrace(), result}
	_, err := BuildCallTree(traces)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmatched trace action and result")
}

// ---------------------------------------------------------------------------
// TC-17  propagateSuccess – three-level all success
// ---------------------------------------------------------------------------

func TestPropagateSuccess_TC17_ThreeLevelAllSuccess(t *testing.T) {
	// Build tree manually: A -> B -> C, all succeed
	a := &CallFrame{Data: &CallTraceData{
		BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()},
		Result:        types.CallResult{Outcome: types.OUTCOME_SUCCESS},
	}}
	b := &CallFrame{Data: &CallTraceData{
		BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()},
		Result:        types.CallResult{Outcome: types.OUTCOME_SUCCESS},
	}, Parent: a}
	c := &CallFrame{Data: &CallTraceData{
		BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()},
		Result:        types.CallResult{Outcome: types.OUTCOME_SUCCESS},
	}, Parent: b}
	a.Children = []*CallFrame{b}
	b.Children = []*CallFrame{c}

	propagateSuccess(a, true)

	assert.True(t, a.fullChainSuccess)
	assert.True(t, b.fullChainSuccess)
	assert.True(t, c.fullChainSuccess)
}

// ---------------------------------------------------------------------------
// TC-18  propagateSuccess – middle node fails, child inherits false
// ---------------------------------------------------------------------------

func TestPropagateSuccess_TC18_MiddleNodeFail(t *testing.T) {
	a := &CallFrame{Data: &CallTraceData{
		BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()},
		Result:        types.CallResult{Outcome: types.OUTCOME_SUCCESS},
	}}
	b := &CallFrame{Data: &CallTraceData{
		BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()},
		Result:        types.CallResult{Outcome: types.OUTCOME_REVERTED}, // B fails
	}, Parent: a}
	c := &CallFrame{Data: &CallTraceData{
		BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()},
		Result:        types.CallResult{Outcome: types.OUTCOME_SUCCESS},
	}, Parent: b}
	a.Children = []*CallFrame{b}
	b.Children = []*CallFrame{c}

	propagateSuccess(a, true)

	assert.True(t, a.fullChainSuccess)
	assert.False(t, b.fullChainSuccess)
	assert.False(t, c.fullChainSuccess) // inherited from B
}

// ---------------------------------------------------------------------------
// TC-19  propagateSuccess – root fails, entire chain false
// ---------------------------------------------------------------------------

func TestPropagateSuccess_TC19_RootFail(t *testing.T) {
	a := &CallFrame{Data: &CallTraceData{
		BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()},
		Result:        types.CallResult{Outcome: types.OUTCOME_REVERTED}, // root fails
	}}
	b := &CallFrame{Data: &CallTraceData{
		BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()},
		Result:        types.CallResult{Outcome: types.OUTCOME_SUCCESS},
	}, Parent: a}
	c := &CallFrame{Data: &CallTraceData{
		BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()},
		Result:        types.CallResult{Outcome: types.OUTCOME_SUCCESS},
	}, Parent: b}
	a.Children = []*CallFrame{b}
	b.Children = []*CallFrame{c}

	propagateSuccess(a, true)

	assert.False(t, a.fullChainSuccess)
	assert.False(t, b.fullChainSuccess)
	assert.False(t, c.fullChainSuccess)
}

// ---------------------------------------------------------------------------
// TC-20  Walk – pre-order traversal A -> B -> D -> C
// ---------------------------------------------------------------------------

func TestWalk_TC20_PreOrder(t *testing.T) {
	//   A
	//  / \
	// B   C
	// |
	// D
	makeNode := func(id string) *CallFrame {
		tr := callTrace()
		tr.TransactionHash = types.Hash("0x" + id + strings.Repeat("0", 63-len(id)))
		return &CallFrame{Data: &CallTraceData{
			BaseTraceData: &BaseTraceData{LocalizedTrace: tr},
		}}
	}

	a := makeNode("a")
	b := makeNode("b")
	c := makeNode("c")
	d := makeNode("d")

	b.Children = []*CallFrame{d}
	d.Parent = b
	a.Children = []*CallFrame{b, c}
	b.Parent = a
	c.Parent = a

	var visited []types.Hash
	err := Walk([]*CallFrame{a}, func(n *CallFrame) error {
		visited = append(visited, n.Data.Trace().TransactionHash)
		return nil
	})
	require.NoError(t, err)

	expected := []types.Hash{
		a.Data.Trace().TransactionHash,
		b.Data.Trace().TransactionHash,
		d.Data.Trace().TransactionHash,
		c.Data.Trace().TransactionHash,
	}
	assert.Equal(t, expected, visited)
}

// ---------------------------------------------------------------------------
// TC-21  Walk – early termination on error
// ---------------------------------------------------------------------------

func TestWalk_TC21_EarlyTerminationOnError(t *testing.T) {
	sentinel := errors.New("stop")

	a := &CallFrame{Data: &CallTraceData{BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()}}}
	b := &CallFrame{Data: &CallTraceData{BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()}}, Parent: a}
	c := &CallFrame{Data: &CallTraceData{BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()}}, Parent: b}
	a.Children = []*CallFrame{b}
	b.Children = []*CallFrame{c}

	visited := 0
	err := Walk([]*CallFrame{a}, func(n *CallFrame) error {
		visited++
		if visited == 2 { // stop at B
			return sentinel
		}
		return nil
	})

	assert.ErrorIs(t, err, sentinel)
	assert.Equal(t, 2, visited) // A and B only; C never visited
}

// ---------------------------------------------------------------------------
// TC-22  Walk – multiple roots traversal order
// ---------------------------------------------------------------------------

func TestWalk_TC22_MultipleRootsOrder(t *testing.T) {
	newLeaf := func() *CallFrame {
		return &CallFrame{Data: &CallTraceData{
			BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()},
		}}
	}

	a, b, c, d := newLeaf(), newLeaf(), newLeaf(), newLeaf()
	a.Children = []*CallFrame{b}
	b.Parent = a
	c.Children = []*CallFrame{d}
	d.Parent = c

	order := make([]*CallFrame, 0, 4)
	err := Walk([]*CallFrame{a, c}, func(n *CallFrame) error {
		order = append(order, n)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []*CallFrame{a, b, c, d}, order)
}

// ---------------------------------------------------------------------------
// TC-23  TreeBuilder.Peek on empty stack returns nil
// ---------------------------------------------------------------------------

func TestTreeBuilder_TC23_PeekEmptyStack(t *testing.T) {
	b := NewTreeBuilder[TraceData]()
	assert.Nil(t, b.Peek())
}

// ---------------------------------------------------------------------------
// TC-24  TreeBuilder.Pop on empty stack returns error
// ---------------------------------------------------------------------------

func TestTreeBuilder_TC24_PopEmptyStack(t *testing.T) {
	b := NewTreeBuilder[TraceData]()
	_, err := b.Pop()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pop empty stack")
}

// ---------------------------------------------------------------------------
// TC-25  TreeBuilder.StackSize tracks Push / Pop
// ---------------------------------------------------------------------------

func TestTreeBuilder_TC25_StackSize(t *testing.T) {
	b := NewTreeBuilder[TraceData]()
	assert.Equal(t, 0, b.StackSize())

	makeData := func() TraceData {
		return &CallTraceData{
			BaseTraceData: &BaseTraceData{LocalizedTrace: callTrace()},
		}
	}

	b.Push(makeData())
	assert.Equal(t, 1, b.StackSize())

	b.Push(makeData())
	assert.Equal(t, 2, b.StackSize())

	b.Push(makeData())
	assert.Equal(t, 3, b.StackSize())

	_, err := b.Pop()
	require.NoError(t, err)
	assert.Equal(t, 2, b.StackSize())
}

// ---------------------------------------------------------------------------
// Integration: JSON fixture from the problem statement
// ---------------------------------------------------------------------------

func TestBuildCallTree_Integration_JSONFixture(t *testing.T) {
	// Mirrors the cfxTraces array from the provided JSON response:
	//   internal_transfer → call → internal_transfer → call_result(success) → internal_transfer
	traces := []*types.LocalizedTrace{
		internalTransferTrace(),                // gas pre-payment
		callTrace(),                            // deposit() call
		internalTransferTrace(),                // staking balance transfer
		callResultTrace(types.OUTCOME_SUCCESS), // call_result
		internalTransferTrace(),                // gas refund
	}

	roots, err := BuildCallTree(traces)
	require.NoError(t, err)
	require.Len(t, roots, 1, "exactly one call root")

	root := roots[0]
	data, ok := root.Data.(*CallTraceData)
	require.True(t, ok)
	assert.Equal(t, types.OUTCOME_SUCCESS, data.Result.Outcome)
	assert.True(t, root.fullChainSuccess)
	assert.Empty(t, root.Children, "internal_transfers are not tree nodes")
}
