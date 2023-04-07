package virtualfilter

import (
	"container/list"

	"github.com/ethereum/go-ethereum/common"
	lru "github.com/hashicorp/golang-lru"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

var (
	errReorgBlockNotMatched = errors.New("reorg block not matched")
)

// ethFilterChain simulates a evm space virtual filter blockchain
type ethFilterChain struct {
	*filterChainBase

	// cache to hold block hashes for filter block full of event logs, once whose
	// size exceeded the limit, eviction will be enforced.
	fullBlockhashCache *lru.Cache
}

func newEthFilterChain(maxFilterBlocks int) *ethFilterChain {
	fc := &ethFilterChain{}
	fc.filterChainBase = newFilterChainBase(fc)

	if maxFilterBlocks > 0 { // init full filter block evict cache
		fbhCache, _ := lru.NewWithEvict(maxFilterBlocks, fc.onFilterBlockCacheEvict)
		fc.fullBlockhashCache = fbhCache
	}

	return fc
}

func (fc *ethFilterChain) merge(fchanges filterChanges) error {
	chainedBlocksList := parseEthFilterChanges(fchanges.(*types.FilterChanges))
	if len(chainedBlocksList) == 0 {
		return nil
	}

	// update the virtual filter blockchain
	for i := range chainedBlocksList {
		head := chainedBlocksList[i].Front()
		if block := head.Value.(*ethFilterBlock); block.reorged {
			// reorg the chain
			if err := fc.reorg(chainedBlocksList[i]); err != nil {
				return errors.WithMessage(err, "failed to reorg filter chain")
			}
		} else if err := fc.extend(chainedBlocksList[i]); err != nil {
			// extend the chain
			return errors.WithMessage(err, "failed to extend filter chain")
		}
	}

	return nil
}

// reorgs the filter chain
func (fc *ethFilterChain) reorg(reverted *list.List) error {
	rnode := fc.back()

	for ele := reverted.Front(); ele != nil; ele = ele.Next() {
		rblock := ele.Value.(*ethFilterBlock)

		if rnode == nil && !fc.exists(rblock.blockHash) {
			rnode = fc.pushFront(rblock)
		}

		if rnode == nil || !rnode.pointedAt(rblock) {
			return errReorgBlockNotMatched
		}

		rnode.filterCursor = rblock
		rnode = rnode.getPrev()
	}

	// rewind the blockchain
	return fc.rewind(rnode)
}

// extends the filter chain
func (fc *ethFilterChain) extend(extended *list.List) error {
	for ele := extended.Front(); ele != nil; ele = ele.Next() {
		nblock := ele.Value.(*ethFilterBlock)

		oldN, existed := fc.hashToNodes[nblock.hash()]
		if !existed { // block with hash not existed
			fc.pushBack(nblock)
			continue
		}

		// otherwise, this may be due to the block has been re-orged but re-mined to the canonical
		// chain afterwards.

		// make sure the old block is re-orged
		if oldBlock := oldN.filterCursor.(*ethFilterBlock); !oldBlock.reorged {
			return errors.Errorf("filter block with hash %v already exists", nblock.blockHash)
		}

		// also make sure the prev of old node is the tail of the canonical chain under such circumstance
		if oldN.getPrev() != fc.back() {
			return errors.Errorf(
				"re-orged filter block with hash %v must be extended after the canonical tail",
				nblock.blockHash,
			)
		}

		oldN.filterCursor = nblock
		fc.filterChainBase.pushBack(oldN)
	}

	return nil
}

// traverses the filter chain from some specified cursor
func (fc *ethFilterChain) traverse(cursor filterCursor, iterator filterChainIterator) error {
	if fc.genesis == nil { // unexploited filter chain?
		return nil
	}

	var startNode, node *filterNode

	if cursor == nil {
		// starting from genesis node if nil cursor specified
		startNode = fc.genesis
	} else if startNode = fc.hashToNodes[cursor.hash()]; startNode == nil {
		// no filter node found with the cursor
		return errBadFilterCursor
	}

	// move backforward to the canonical chain if starting from fork chain
	for node = startNode; node != nil; node = node.getPrev() {
		if block := node.filterCursor.(*ethFilterBlock); !block.reorged {
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
			node = fc.front()
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

// exists checks if the filter block inserted before
func (fc *ethFilterChain) exists(blockHash common.Hash) bool {
	_, ok := fc.hashToNodes[blockHash.String()]
	return ok
}

// pushFront inserts new block at the front of the chain
func (fc *ethFilterChain) pushFront(block *ethFilterBlock) *filterNode {
	node := newFilterNode(fc, block)
	fc.filterChainBase.pushFront(node)

	if fc.fullBlockhashCache != nil {
		fc.fullBlockhashCache.Add(block.hash(), struct{}{})
	}

	return node
}

// pushBack inserts new block at the back of the chain
func (fc *ethFilterChain) pushBack(block *ethFilterBlock) *filterNode {
	node := newFilterNode(fc, block)
	fc.filterChainBase.pushBack(node)

	if fc.fullBlockhashCache != nil {
		fc.fullBlockhashCache.Add(block.hash(), struct{}{})
	}

	return node
}

// onFilterBlockCacheEvict callbacks to clean event logs of evicted full filter block
func (fc *ethFilterChain) onFilterBlockCacheEvict(key, value interface{}) {
	bh := key.(string)

	if fn, ok := fc.hashToNodes[bh]; ok {
		// clean event logs for the filter block
		fn.filterCursor.(*ethFilterBlock).logs = nil
	}
}

type ethFilterCursor struct {
	blockNum  uint64
	blockHash common.Hash
}

// implements `filterCursor` interface

func (b ethFilterCursor) number() uint64 {
	return b.blockNum
}

func (b ethFilterCursor) hash() string {
	return b.blockHash.String()
}

// ethFilterBlock evm space virtual filter block with event logs inside.
type ethFilterBlock struct {
	ethFilterCursor

	reorged bool        // whether the block is re-organized
	logs    []types.Log // logs contained within this block
}

func newEthFilterBlockFromLogs(logs []types.Log) *ethFilterBlock {
	if len(logs) == 0 {
		return nil
	}

	return &ethFilterBlock{
		ethFilterCursor: ethFilterCursor{
			blockNum:  logs[0].BlockNumber,
			blockHash: logs[0].BlockHash,
		},
		reorged: logs[0].Removed,
		logs:    logs,
	}
}

// parseEthFilterChanges parses evm space filter chagnes to return multiple linked lists
// of filter block grouped by `Removed` property of event log.
func parseEthFilterChanges(changes *types.FilterChanges) []*list.List {
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

		var bhs []string
		blockLogs := make(map[string][]types.Log)

		for j := range logs {
			blockHash := logs[j].BlockHash.Hex()

			if _, ok := blockLogs[blockHash]; !ok {
				bhs = append(bhs, blockHash)
			}

			blockLogs[blockHash] = append(blockLogs[blockHash], logs[j])
		}

		for k := range bhs {
			block := newEthFilterBlockFromLogs(blockLogs[bhs[k]])
			blockLists[i].PushBack(block)
		}
	}

	return blockLists
}
