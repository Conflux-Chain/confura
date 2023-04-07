package virtualfilter

import (
	"fmt"

	"github.com/pkg/errors"
)

var (
	errRewindNodeNotFound = errors.New("rewind node not found")
	errBadFilterCursor    = errors.New("bad filter cursor")
)

type filterCursor interface {
	number() uint64 // node number
	hash() string   // node hash
}

type filterChainIterator func(node *filterNode, forkPoint bool) bool

type filterChain interface {
	// snapshots filter cursor of the latest node
	snapshotLatestCursor() filterCursor
	// check if node is on the chain
	hasNode(node *filterNode) bool
	// merge filter changes to the filter chain
	merge(fchanges filterChanges) error
	// traverse the filter chain from specific cursor
	traverse(cursor filterCursor, iterator filterChainIterator) error
}

type filterChainBase struct {
	root        filterNode             // sentinel root node
	genesis     *filterNode            // (the first) genesis chain node
	len         int                    // canonical chain length
	hashToNodes map[string]*filterNode // hash => filter node
}

func newFilterChainBase(derived filterChain) *filterChainBase {
	fcb := &filterChainBase{
		hashToNodes: make(map[string]*filterNode),
	}

	// init root node
	fcb.root = filterNode{chain: derived}
	fcb.root.prev, fcb.root.next = &fcb.root, &fcb.root

	return fcb
}

// snapshotLatestCursor snapshots the latest cursor
func (fc *filterChainBase) snapshotLatestCursor() filterCursor {
	if tail := fc.back(); tail != nil {
		return tail.filterCursor
	}

	return nil
}

// hasNode check if node on the filter chain
func (c *filterChainBase) hasNode(node *filterNode) bool {
	if node == nil || node == &c.root { // sentinel node?
		return false
	}

	_, ok := c.hashToNodes[node.hash()]
	return ok
}

// front returns the first node of the filter chain or nil if the chain is empty.
func (fc *filterChainBase) front() *filterNode {
	if fc.len == 0 {
		return nil
	}

	return fc.root.next
}

// back returns the last node of the filter chain or nil if the chain is empty.
func (fc *filterChainBase) back() *filterNode {
	if fc.len == 0 {
		return nil
	}

	return fc.root.prev
}

// pushFront inserts new node at the front of the chain
func (fc *filterChainBase) pushFront(node *filterNode) {
	fc.insert(node, &fc.root)
}

// pushBack inserts new node at the back of the chain
func (fc *filterChainBase) pushBack(node *filterNode) {
	fc.insert(node, fc.root.prev)
}

// insert inserts node after at and increments fc.len.
func (fc *filterChainBase) insert(n, at *filterNode) {
	if fc.genesis == nil {
		fc.genesis = n
	}

	fc.hashToNodes[n.hash()] = n

	n.prev = at
	n.next = at.next
	n.prev.next = n
	n.next.prev = n

	fc.len++
}

// rewind reverts the canonical chain to the specified node
func (fc *filterChainBase) rewind(node *filterNode) error {
	if node == nil { // rewind until to the root
		fc.root.prev, fc.root.next = &fc.root, &fc.root
		fc.len = 0

		return nil
	}

	for steps, tail := 0, fc.back(); tail != nil; {
		if tail == node {
			fc.root.prev = node
			node.next = &fc.root

			fc.len -= steps
			return nil
		}

		steps++
		tail = tail.getPrev()
	}

	return errRewindNodeNotFound
}

// traverses the filter chain from the starting cursor, and prints the node info
// (eg., block number, block hash etc.), mainly used for debugging.
func printFilterChain(fc filterChain, cursor filterCursor) {
	err := fc.traverse(cursor, func(node *filterNode, forkPoint bool) bool {
		var forktag string
		if forkPoint {
			forktag = "[fork]"
		}

		if node != nil {
			fmt.Printf("-> #%d(%v)%v", node.number(), node.hash(), forktag)
		} else {
			fmt.Printf("-> nil%v", forktag)
		}

		return true
	})

	if err == nil {
		fmt.Println("->root")
	} else {
		fmt.Println("traversal error: ", err)
	}
}

type filterNode struct {
	filterCursor

	chain      filterChain // the chain this node belongs to
	prev, next *filterNode // prev or next node
}

func newFilterNode(chain filterChain, cursor filterCursor) *filterNode {
	return &filterNode{chain: chain, filterCursor: cursor}
}

// pointedAt check if the node pointed at the specified cursor
func (n *filterNode) pointedAt(fc filterCursor) bool {
	return n.number() == fc.number() && n.hash() == fc.hash()
}

// getNext returns the next chain node or nil.
func (n *filterNode) getNext() *filterNode {
	if p := n.next; n.chain != nil && n.chain.hasNode(p) {
		return p
	}

	return nil
}

// getPrev returns the previous chain node or nil.
func (n *filterNode) getPrev() *filterNode {
	if p := n.prev; n.chain != nil && n.chain.hasNode(p) {
		return p
	}

	return nil
}
