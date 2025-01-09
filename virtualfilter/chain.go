package virtualfilter

import (
	"github.com/pkg/errors"
)

var (
	nilFilterCursor = filterCursor{}

	errRewindNodeNotFound = errors.New("rewind node not found")
	errBadFilterCursor    = errors.New("bad filter cursor")
)

type filterNodable interface {
	cursor() filterCursor // pointer cursor
	reorged() bool        // node reorged or not?
}

type filterCursor struct {
	height uint64 // block height
	hash   string // block hash
}

type filterChainIterator func(node *filterNode, forkPoint bool) bool

type filterChain interface {
	// snapshotLatestCursor filter cursor of the latest node
	snapshotLatestCursor() filterCursor
	// check if node is on the chain
	hasNode(node *filterNode) bool
	// merge filter changes to the filter chain
	merge(fchanges filterChanges) error
	// traverse the filter chain from specific cursor
	traverse(cursor filterCursor, iterator filterChainIterator) error
	// print all nodes along the chain from specific cursor
	print(cursor filterCursor)
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
		return tail.cursor()
	}

	return nilFilterCursor
}

// hasNode check if node on the filter chain
func (c *filterChainBase) hasNode(node *filterNode) bool {
	if node == nil || node == &c.root { // sentinel node?
		return false
	}

	_, ok := c.hashToNodes[node.cursor().hash]
	return ok
}

// traverses the filter chain from some specified cursor
func (c *filterChainBase) traverse(cursor filterCursor, iterator filterChainIterator) error {
	if c.genesis == nil { // unexploited filter chain?
		return nil
	}

	var startNode, node *filterNode

	if cursor == nilFilterCursor {
		// starting from genesis node if nil cursor specified
		startNode = c.genesis
	} else if startNode = c.hashToNodes[cursor.hash]; startNode == nil {
		// no filter node found with the cursor
		return errBadFilterCursor
	}

	// move backforward to the canonical chain if starting from fork chain
	for node = startNode; node != nil; node = node.getPrev() {
		if !node.reorged() {
			break
		}

		if !iterator(node, false) {
			return nil
		}
	}

	// fork chain iterated over
	if node != startNode {
		// iterate fork point node on the canonical chain, note it might be nil
		// which means no intersection.
		if !iterator(node, true) {
			return nil
		}

		if node != nil {
			node = node.getNext()
		} else {
			// no intersection between the fork and canonical chain,
			// let's start from head of the canonical chain.
			node = c.front()
		}
	}

	// move forward along the canonical chain until the end
	for ; node != nil; node = node.getNext() {
		if !iterator(node, false) {
			return nil
		}
	}

	return nil
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

	if bh := n.cursor().hash; len(bh) > 0 {
		fc.hashToNodes[n.cursor().hash] = n
	}

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

type filterNode struct {
	filterNodable

	chain      filterChain // the chain this node belongs to
	prev, next *filterNode // prev or next node
}

func newFilterNode(chain filterChain, body filterNodable) *filterNode {
	return &filterNode{chain: chain, filterNodable: body}
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
