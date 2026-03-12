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

// CallFrame represents a node in the call tree, wrapping a localized trace
// with parent-child relationships and parsed action/result data.
type CallFrame struct {
	*types.LocalizedTrace

	CallAction   *types.Call
	CallResult   *types.CallResult
	CreateAction *types.Create
	CreateResult *types.CreateResult

	Parent   *CallFrame
	Children []*CallFrame

	// fullChainSuccess is precomputed top-down after tree construction.
	// True only if this frame AND every ancestor all succeeded.
	fullChainSuccess bool
}

// IsSuccess checks whether this individual frame completed successfully.
func (f *CallFrame) IsSuccess() bool {
	switch f.Type {
	case types.TRACE_CALL:
		return f.CallResult != nil && f.CallResult.Outcome == types.OUTCOME_SUCCESS
	case types.TRACE_CREATE:
		return f.CreateResult != nil && f.CreateResult.Outcome == types.OUTCOME_SUCCESS
	default:
		return false
	}
}

// IsFullChainSuccess returns the precomputed chain success status.
func (f *CallFrame) IsFullChainSuccess() bool {
	return f.fullChainSuccess
}

// BuildCallTree converts a flat list of localized traces into a forest of CallFrames.
func BuildCallTree(traces []*types.LocalizedTrace) ([]*CallFrame, error) {
	var roots []*CallFrame
	var stack []*CallFrame

	for _, t := range traces {
		switch t.Type {
		case types.TRACE_CALL:
			action := t.Action.(types.Call)
			frame := &CallFrame{
				LocalizedTrace: t,
				CallAction:     &action,
			}
			roots, stack = pushFrame(frame, roots, stack)

		case types.TRACE_CREATE:
			action := t.Action.(types.Create)
			frame := &CallFrame{
				LocalizedTrace: t,
				CreateAction:   &action,
			}
			roots, stack = pushFrame(frame, roots, stack)

		case types.TRACE_CALL_RESULT:
			frame, err := popFrame(stack, types.TRACE_CALL)
			if err != nil {
				return nil, err
			}
			result := t.Action.(types.CallResult)
			frame.CallResult = &result
			stack = stack[:len(stack)-1]

		case types.TRACE_CREATE_RESULT:
			frame, err := popFrame(stack, types.TRACE_CREATE)
			if err != nil {
				return nil, err
			}
			result := t.Action.(types.CreateResult)
			frame.CreateResult = &result
			stack = stack[:len(stack)-1]
		}
	}

	if len(stack) != 0 {
		return nil, errors.New("unmatched call frames remaining on stack")
	}

	// Precompute fullChainSuccess top-down: O(n) total, each node visited once.
	for _, root := range roots {
		precomputeChainSuccess(root, true)
	}

	return roots, nil
}

// precomputeChainSuccess propagates success status from parent to children.
//
//	parentChainSuccess = true means all ancestors (including parent) succeeded.
//	This node's fullChainSuccess = parentChainSuccess && self.IsSuccess().
//	Each node is visited exactly once → O(n) total.
func precomputeChainSuccess(frame *CallFrame, parentChainSuccess bool) {
	frame.fullChainSuccess = parentChainSuccess && frame.IsSuccess()
	for _, child := range frame.Children {
		precomputeChainSuccess(child, frame.fullChainSuccess)
	}
}

func pushFrame(frame *CallFrame, roots []*CallFrame, stack []*CallFrame) ([]*CallFrame, []*CallFrame) {
	if len(stack) > 0 {
		parent := stack[len(stack)-1]
		frame.Parent = parent
		parent.Children = append(parent.Children, frame)
	} else {
		roots = append(roots, frame)
	}
	return roots, append(stack, frame)
}

func popFrame(stack []*CallFrame, expectedType types.TraceType) (*CallFrame, error) {
	if len(stack) == 0 {
		return nil, errors.New("unexpected result: empty stack")
	}

	frame := stack[len(stack)-1]
	if frame.Type != expectedType {
		return nil, errors.Errorf("mismatched trace type: expected %v, got %v", expectedType, frame.Type)
	}

	return frame, nil
}
