package tracelog

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
)

// CallFrame is a tree node holding a polymorphic trace payload.
type CallFrame = TreeNode[TraceData]

// BuildCallTree builds a call tree from flat traces using TreeBuilder.
func BuildCallTree(traces []*types.LocalizedTrace) ([]*CallFrame, error) {
	builder := NewTreeBuilder[TraceData]()

	for _, t := range traces {
		switch t.Type {
		case types.TRACE_CALL:
			builder.Push(&CallTraceData{
				BaseTraceData: &BaseTraceData{LocalizedTrace: t},
				Action:        t.Action.(types.Call),
			})

		case types.TRACE_CREATE:
			builder.Push(&CreateTraceData{
				BaseTraceData: &BaseTraceData{LocalizedTrace: t},
				Action:        t.Action.(types.Create),
			})

		case types.TRACE_CALL_RESULT, types.TRACE_CREATE_RESULT:
			if err := handleTraceResult(builder, t); err != nil {
				return nil, err
			}
		}
	}

	// Validates empty stack + precomputes chain success in O(n)
	return builder.Build()
}

// TreeNode is a generic tree node with precomputed chain-success.
type TreeNode[T TraceData] struct {
	Data     T
	Parent   *TreeNode[T]
	Children []*TreeNode[T]

	// fullChainSuccess is precomputed top-down after tree construction.
	// True only if this frame AND every ancestor all succeeded.
	fullChainSuccess bool
}

// TreeBuilder encapsulates the stack-based tree construction process.
type TreeBuilder[T TraceData] struct {
	roots []*TreeNode[T]
	stack []*TreeNode[T]
}

// NewTreeBuilder creates a new builder.
func NewTreeBuilder[T TraceData]() *TreeBuilder[T] {
	return &TreeBuilder[T]{}
}

// Push creates a child of the current stack top (or a new root) and pushes it onto the stack.
func (b *TreeBuilder[T]) Push(data T) *TreeNode[T] {
	node := &TreeNode[T]{Data: data}
	if len(b.stack) > 0 {
		parent := b.stack[len(b.stack)-1]
		node.Parent = parent
		parent.Children = append(parent.Children, node)
	} else {
		b.roots = append(b.roots, node)
	}
	b.stack = append(b.stack, node)
	return node
}

// Peek returns the current stack top without popping.
func (b *TreeBuilder[T]) Peek() *TreeNode[T] {
	if len(b.stack) > 0 {
		return b.stack[len(b.stack)-1]
	}
	return nil
}

// Pop removes and returns the current stack top.
func (b *TreeBuilder[T]) Pop() (*TreeNode[T], error) {
	if len(b.stack) == 0 {
		return nil, errors.New("pop empty stack")
	}
	top := b.stack[len(b.stack)-1]
	b.stack = b.stack[:len(b.stack)-1]
	return top, nil
}

// StackSize returns the current nesting depth.
func (b *TreeBuilder[T]) StackSize() int {
	return len(b.stack)
}

// Build finalizes the tree, validates the stack is empty,
// and precomputes chain success using the provided predicate. O(n).
func (b *TreeBuilder[T]) Build() ([]*TreeNode[T], error) {
	if len(b.stack) != 0 {
		return nil, errors.Errorf("%d unmatched frames remaining", len(b.stack))
	}
	for _, root := range b.roots {
		propagateSuccess(root, true)
	}
	return b.roots, nil
}

func propagateSuccess[T TraceData](node *TreeNode[T], parentOk bool) {
	node.fullChainSuccess = parentOk && node.Data.IsSuccess()
	for _, child := range node.Children {
		propagateSuccess(child, node.fullChainSuccess)
	}
}

// Walk traverses all trees in pre-order.
func Walk[T TraceData](roots []*TreeNode[T], visit func(*TreeNode[T]) error) error {
	for _, root := range roots {
		if err := walkNode(root, visit); err != nil {
			return err
		}
	}
	return nil
}

func walkNode[T TraceData](node *TreeNode[T], visit func(*TreeNode[T]) error) error {
	if err := visit(node); err != nil {
		return err
	}
	for _, child := range node.Children {
		if err := walkNode(child, visit); err != nil {
			return err
		}
	}
	return nil
}

func handleTraceResult(builder *TreeBuilder[TraceData], result *types.LocalizedTrace) error {
	top, err := builder.Pop()
	if err != nil {
		return err
	}

	switch traceType := top.Data.TraceType(); traceType {
	case types.TRACE_CALL:
		if result.Type != types.TRACE_CALL_RESULT {
			return errors.Errorf("expected call result, got %v", result.Type)
		}

	case types.TRACE_CREATE:
		if result.Type != types.TRACE_CREATE_RESULT {
			return errors.Errorf("expected create result, got %v", result.Type)
		}

	default:
		return errors.Errorf("unexpected trace type %v", traceType)
	}

	action := top.Data.Trace()
	if action.BlockHash != result.BlockHash ||
		action.TransactionHash != result.TransactionHash ||
		action.TransactionPosition != result.TransactionPosition ||
		action.EpochHash != result.EpochHash ||
		action.EpochNumber.ToInt().Cmp(result.EpochNumber.ToInt()) != 0 ||
		action.Valid != result.Valid {
		return errors.Errorf("unmatched trace action and result")
	}

	switch d := top.Data.(type) {
	case *CallTraceData:
		d.Result = result.Action.(types.CallResult)
	case *CreateTraceData:
		d.Result = result.Action.(types.CreateResult)
	default:
		return errors.Errorf("unexpected trace data type %T", top.Data)
	}

	return nil
}
