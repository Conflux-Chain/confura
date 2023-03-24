package virtualfilter

import (
	"container/list"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	lru "github.com/hashicorp/golang-lru"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	errRevertedBlockNotMatched = errors.New("reverted block not matched")
	errRewindBlockNotFound     = errors.New("rewind block not found")
	errBadFilterCursor         = errors.New("bad filter cursor")
)

// parseFilterChanges parses filter chagnes to return multiple linked lists
// of filter block grouped by `Removed` property of event log.
func parseFilterChanges(changes *types.FilterChanges) []*list.List {
	if len(changes.Logs) == 0 {
		return nil
	}

	// split continuous event logs into groups by `Removed` property
	var splitLogs [][]types.Log
	var tmplogs []types.Log

	for i := 0; i < len(changes.Logs); i++ {
		if len(tmplogs) == 0 || tmplogs[0].Removed == changes.Logs[i].Removed {
			tmplogs = append(tmplogs, changes.Logs[i])
			continue
		}

		splitLogs = append(splitLogs, tmplogs)
		tmplogs = []types.Log{changes.Logs[i]}
	}

	splitLogs = append(splitLogs, tmplogs)

	// convert split logs to linked lists of filter blocks
	blockLists := make([]*list.List, len(splitLogs))
	for i, logs := range splitLogs {
		blockLists[i] = list.New()

		// create the head block of linked list
		block := NewFilterBlockFromLog(&logs[0])
		blockLists[i].PushBack(block)

		for j := range logs {
			if !block.Include(&logs[j]) { // start new block
				block = NewFilterBlockFromLog(&logs[j])
				blockLists[i].PushBack(block)
			}

			block.AppendLogs(logs[j])
		}
	}

	return blockLists
}

// FilterNode chain node which wraps block and link info
type FilterNode struct {
	*FilterBlock

	chain      *FilterChain // the chain this node belongs to
	prev, next *FilterNode
}

// Next returns the next chain node or nil.
func (n *FilterNode) Next() *FilterNode {
	if p := n.next; n.chain != nil && p != &n.chain.root {
		return p
	}

	return nil
}

// Prev returns the previous chain node or nil.
func (n *FilterNode) Prev() *FilterNode {
	if p := n.prev; n.chain != nil && p != &n.chain.root {
		return p
	}
	return nil
}

// FilterBlock virtual filter block with event logs inside.
type FilterBlock struct {
	FilterCursor

	reorg bool        // whether the block is re-organized
	logs  []types.Log // logs contained within this block
}

func NewFilterBlockFromLog(log *types.Log) *FilterBlock {
	return &FilterBlock{
		FilterCursor: FilterCursor{
			blockNum: log.BlockNumber, blockHash: log.BlockHash,
		},
		reorg: log.Removed,
	}
}

func (fb *FilterBlock) AppendLogs(logs ...types.Log) {
	fb.logs = append(fb.logs, logs...)
}

func (fb *FilterBlock) Include(log *types.Log) bool {
	return fb.blockNum == log.BlockNumber && fb.blockHash == log.BlockHash
}

func (fb *FilterBlock) Match(block *FilterBlock) bool {
	return fb.blockNum == block.blockNum && fb.blockHash == block.blockHash
}

// FilterChain simulates a virtual filter blockchain
type FilterChain struct {
	root        FilterNode                  // sentinel root node
	genesis     *FilterNode                 // (the first) genesis chain node
	len         int                         // canonical chain length
	hashToNodes map[common.Hash]*FilterNode // block hash => filter node

	// cache to hold block hashes for filter block full of event logs, once whose
	// size exceeded the limit, eviction will be enforced.
	fullBlockhashCache *lru.Cache
}

func NewFilterChain(maxFilterBlocks int) *FilterChain {
	fc := &FilterChain{
		hashToNodes: make(map[common.Hash]*FilterNode),
	}

	if maxFilterBlocks > 0 {
		// init full filter block cache
		fbhCache, _ := lru.NewWithEvict(maxFilterBlocks, fc.onFilterBlockCacheEvict)
		fc.fullBlockhashCache = fbhCache
	}

	// init root node
	fc.root = FilterNode{chain: fc}
	fc.root.prev, fc.root.next = &fc.root, &fc.root

	return fc
}

func (fc *FilterChain) Merge(changes *types.FilterChanges) error {
	chainedBlocksList := parseFilterChanges(changes)
	if len(chainedBlocksList) == 0 {
		return nil
	}

	// update the virtual filter blockchain
	for i := range chainedBlocksList {
		if head := chainedBlocksList[i].Front(); head.Value.(*FilterBlock).reorg {
			// reorg the chain
			if err := fc.Reorg(chainedBlocksList[i]); err != nil {
				return errors.WithMessage(err, "failed to reorg filter chain")
			}

			continue
		}

		// extend the chain
		if err := fc.Extend(chainedBlocksList[i]); err != nil {
			return errors.WithMessage(err, "failed to extend filter chain")
		}
	}

	return nil
}

func (fc *FilterChain) Reorg(reverted *list.List) error {
	rnode := fc.Back()

	for ele := reverted.Front(); ele != nil; ele = ele.Next() {
		rblock := ele.Value.(*FilterBlock)

		if rnode == nil && !fc.exists(rblock.blockHash) {
			rnode = fc.PushFront(rblock)
		}

		if rnode == nil || !rnode.Match(rblock) {
			return errRevertedBlockNotMatched
		}

		rnode.FilterBlock = rblock
		rnode = rnode.Prev()
	}

	// rewind the blockchain
	return fc.Rewind(rnode)
}

func (fc *FilterChain) Extend(extended *list.List) error {
	for ele := extended.Front(); ele != nil; ele = ele.Next() {
		nblock := ele.Value.(*FilterBlock)

		if fnode, ok := fc.hashToNodes[nblock.blockHash]; ok {
			logrus.WithFields(logrus.Fields{
				"blockHash": fnode.blockHash,
				"blockNum":  fnode.blockNum,
				"reorg":     fnode.reorg,
				"prev":      fnode.prev,
				"next":      fnode.next,
				"logs":      fnode.logs,
			}).Info("!!! Virtual filter block already existed")
		}

		if fc.exists(nblock.blockHash) {
			return errors.Errorf("filter block with hash %v already exists", nblock.blockHash)
		}

		fc.PushBack(nblock)
	}

	return nil
}

type FilterChainIteratorFunc func(node *FilterNode, forkPoint bool) bool

// Traverse traverses the filter chain from some specified cursor
func (fc *FilterChain) Traverse(cursor FilterCursor, iterator FilterChainIteratorFunc) error {
	if fc.genesis == nil { // unexploited filter chain?
		return nil
	}

	var startNode, node *FilterNode

	if cursor == nilFilterCursor {
		// starting from genesis node if nil cursor specified
		startNode = fc.genesis
	} else if startNode = fc.hashToNodes[cursor.blockHash]; startNode == nil {
		// no filter node found with the cursor
		return errBadFilterCursor
	}

	// move backforward to the canonical chain if starting from fork chain
	for node = startNode; node != nil && node.reorg; node = node.Prev() {
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
			node = node.Next()
		} else {
			// no intersection between the fork and canonical chain,
			// let's start from head of the canonical chain.
			node = fc.Front()
		}
	}

	// move forward along the canonical chain until the end
	for ; node != nil; node = node.Next() {
		if !iterator(node, false) {
			return nil
		}
	}

	return nil
}

// Front returns the first node of the filter chain or nil if the chain is empty.
func (fc *FilterChain) Front() *FilterNode {
	if fc.len == 0 {
		return nil
	}

	return fc.root.next
}

// Back returns the last node of the filter chain or nil if the chain is empty.
func (fc *FilterChain) Back() *FilterNode {
	if fc.len == 0 {
		return nil
	}

	return fc.root.prev
}

// SnapshotCurrentCursor snapshots the latest cursor
func (fc *FilterChain) SnapshotCurrentCursor() FilterCursor {
	if tail := fc.Back(); tail != nil {
		return tail.FilterCursor
	}

	return nilFilterCursor
}

// PushFront inserts new node at the front of the chain and returns it
func (fc *FilterChain) PushFront(block *FilterBlock) *FilterNode {
	node := &FilterNode{
		FilterBlock: block, chain: fc,
	}
	return fc.insert(node, &fc.root)
}

// PushFront inserts new node at the back of the chain and returns it
func (fc *FilterChain) PushBack(block *FilterBlock) *FilterNode {
	node := &FilterNode{
		FilterBlock: block, chain: fc,
	}

	return fc.insert(node, fc.root.prev)
}

// insert inserts node after at, increments fc.len, and returns the inserted node.
func (fc *FilterChain) insert(n, at *FilterNode) *FilterNode {
	if fc.genesis == nil {
		fc.genesis = n
	}

	fc.hashToNodes[n.blockHash] = n

	if fc.fullBlockhashCache != nil {
		fc.fullBlockhashCache.Add(n.blockHash, struct{}{})
	}

	n.prev = at
	n.next = at.next
	n.prev.next = n
	n.next.prev = n

	fc.len++
	return n
}

// exists checks if the filter block inserted before
func (fc *FilterChain) exists(blockHash common.Hash) bool {
	_, ok := fc.hashToNodes[blockHash]
	return ok
}

// Rewind reverts the canonical chain to the specified node
func (fc *FilterChain) Rewind(node *FilterNode) error {
	if node == nil { // rewind until to the root
		fc.root.prev, fc.root.next = &fc.root, &fc.root
		fc.len = 0

		return nil
	}

	for steps, tail := 0, fc.Back(); tail != nil; {
		if tail == node {
			fc.root.prev = node
			node.next = &fc.root

			fc.len -= steps
			return nil
		}

		steps++
		tail = tail.Prev()
	}

	return errRewindBlockNotFound
}

// print traverses the filter chain from the starting cursor, and prints the node info
// (eg., block number, block hash etc.), mainly used for debugging.
func (fc *FilterChain) print(cursor FilterCursor) {
	err := fc.Traverse(cursor, func(node *FilterNode, forkPoint bool) bool {
		var forktag string
		if forkPoint {
			forktag = "[fork]"
		}

		if node != nil {
			fmt.Printf("-> #%d(%v)%v", node.blockNum, node.blockHash, forktag)
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

// onFilterBlockCacheEvict callbacks to clean event logs of evicted full filter block
func (fc *FilterChain) onFilterBlockCacheEvict(key, value interface{}) {
	bh := key.(common.Hash)

	if fn, ok := fc.hashToNodes[bh]; ok {
		fn.logs = nil // clean event logs for the filter block
	}
}
