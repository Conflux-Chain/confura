package virtualfilter

import (
	"container/list"
	"fmt"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
)

// cfxFilterChain simulates a core space virtual filter blockchain
type cfxFilterChain struct {
	*filterChainBase

	// cache to hold block hashes for filter epoch full of event logs, once whose
	// size exceeded the limit, eviction will be enforced.
	fullBlockhashCache *lru.Cache
}

func newCfxFilterChain(maxFullFilterEpochs int) *cfxFilterChain {
	fc := &cfxFilterChain{}
	fc.filterChainBase = newFilterChainBase(fc)

	if maxFullFilterEpochs > 0 { // init full filter epoch evict cache
		fbhCache, _ := lru.NewWithEvict(maxFullFilterEpochs, fc.onFilterEpochCacheEvict)
		fc.fullBlockhashCache = fbhCache
	}

	return fc
}

func (fc *cfxFilterChain) merge(fchanges filterChanges) error {
	chainedEpochsList := parseCfxFilterChanges(fchanges.(*types.CfxFilterChanges))
	if len(chainedEpochsList) == 0 {
		return nil
	}

	// update the virtual filter blockchain
	for i := range chainedEpochsList {
		head := chainedEpochsList[i].Front()
		if epoch := head.Value.(*cfxFilterEpoch); epoch.reorged() {
			// reorg the chain
			if err := fc.reorg(chainedEpochsList[i]); err != nil {
				return errors.WithMessage(err, "failed to reorg filter chain")
			}
		} else if err := fc.extend(chainedEpochsList[i]); err != nil {
			// extend the chain
			return errors.WithMessage(err, "failed to extend filter chain")
		}
	}

	return nil
}

// reorgs the filter chain
func (fc *cfxFilterChain) reorg(reverted *list.List) error {
	for ele := reverted.Front(); ele != nil; ele = ele.Next() {
		repoch := ele.Value.(*cfxFilterEpoch)

		rnode := fc.back()
		if rnode == nil { // push the reorg epoch anyway
			fc.pushFront(repoch)
		}

		for rnode != nil { // locate the node to which reverted until
			filterEpoch := rnode.filterNodable.(*cfxFilterEpoch)
			if filterEpoch.epochNum <= repoch.revertedTo {
				break
			}

			filterEpoch.revertedTo = repoch.revertedTo
			rnode = rnode.getPrev()
		}

		// reorg the canonical chain
		if err := fc.rewind(rnode); err != nil {
			return err
		}
	}

	return nil
}

// extends the filter chain
func (fc *cfxFilterChain) extend(extended *list.List) error {
	for ele := extended.Front(); ele != nil; ele = ele.Next() {
		nepoch := ele.Value.(*cfxFilterEpoch)

		oldN, existed := fc.hashToNodes[nepoch.blockHash.String()]
		if !existed { // epoch with hash not existed
			fc.pushBack(nepoch)
			continue
		}

		// otherwise, this may be due to the epoch has been re-orged but re-mined to the canonical
		// chain afterwards.

		// make sure the old epoch is re-orged
		if oldEpoch := oldN.filterNodable.(*cfxFilterEpoch); !oldEpoch.reorged() {
			return errors.Errorf("filter epoch with hash %v already exists", nepoch.blockHash)
		}

		// also make sure the prev of old node is the tail of the canonical chain under such circumstance
		if oldN.getPrev() != fc.back() {
			return errors.Errorf(
				"re-orged filter epoch with hash %v must be extended after the canonical tail",
				nepoch.blockHash,
			)
		}

		oldN.filterNodable = nepoch
		fc.filterChainBase.pushBack(oldN)

		if fc.fullBlockhashCache != nil {
			fc.fullBlockhashCache.Add(nepoch.blockHash, struct{}{})
		}
	}

	return nil
}

// pushFront inserts new epoch at the front of the chain
func (fc *cfxFilterChain) pushFront(epoch *cfxFilterEpoch) *filterNode {
	node := newFilterNode(fc, epoch)
	fc.filterChainBase.pushFront(node)

	if len(epoch.blockHash) > 0 && fc.fullBlockhashCache != nil {
		fc.fullBlockhashCache.Add(epoch.blockHash, struct{}{})
	}

	return node
}

// pushBack inserts new epoch at the back of the chain
func (fc *cfxFilterChain) pushBack(epoch *cfxFilterEpoch) *filterNode {
	node := newFilterNode(fc, epoch)
	fc.filterChainBase.pushBack(node)

	if len(epoch.blockHash) > 0 && fc.fullBlockhashCache != nil {
		fc.fullBlockhashCache.Add(epoch.blockHash, struct{}{})
	}

	return node
}

// onFilterEpochCacheEvict callbacks to clean event logs of evicted full filter epoch
func (fc *cfxFilterChain) onFilterEpochCacheEvict(key, value any) {
	bh := key.(types.Hash)

	if fn, ok := fc.hashToNodes[bh.String()]; ok {
		// clean event logs for the filter epoch
		fn.filterNodable.(*cfxFilterEpoch).logs = nil
	}
}

// traverses the filter chain from the starting cursor, and prints the node info
// (eg., epoch number, block hash etc.), mainly used for debugging.
func (fc *cfxFilterChain) print(cursor filterCursor) {
	err := fc.traverse(cursor, func(node *filterNode, forkPoint bool) bool {
		var forktag string
		if forkPoint {
			forktag = "[fork]"
		}

		if node == nil {
			fmt.Printf("-> nil%v", forktag)
			return true
		}

		fepoch := node.filterNodable.(*cfxFilterEpoch)
		if fepoch.reorged() {
			fmt.Printf("-> reorg[#%d]%v", fepoch.revertedTo, forktag)
			return true
		}

		fmt.Printf("-> #%d(%v)%v", node.cursor().height, node.cursor().hash, forktag)
		return true
	})

	if err == nil {
		fmt.Println("->root")
	} else {
		fmt.Println("traversal error: ", err)
	}
}

// cfxFilterEpoch core space virtual filter epoch with event logs inside.
type cfxFilterEpoch struct {
	epochNum  uint64
	blockHash types.Hash

	revertedTo uint64      // reverted to epoch number, 0 means no reorg
	logs       []types.Log // logs contained within this block
}

func newCfxFilterEpochFromLogs(logs []types.Log) *cfxFilterEpoch {
	if len(logs) == 0 {
		return nil
	}

	return &cfxFilterEpoch{
		epochNum:  logs[0].EpochNumber.ToInt().Uint64(),
		blockHash: *logs[0].BlockHash,
		logs:      logs,
	}
}

func newCfxFilterEpochWithReorg(revertedTo uint64) *cfxFilterEpoch {
	return &cfxFilterEpoch{revertedTo: revertedTo}
}

// implements `filterNodable`

func (fe *cfxFilterEpoch) cursor() filterCursor {
	return filterCursor{
		height: fe.epochNum,
		hash:   fe.blockHash.String(),
	}
}

func (fe *cfxFilterEpoch) reorged() bool {
	return fe.revertedTo > 0
}

// parseCfxFilterChanges parses core space filter changes to return multiple linked lists
// of filter epoch grouped by if contained logs are reverted or not.
func parseCfxFilterChanges(changes *types.CfxFilterChanges) []*list.List {
	if len(changes.Logs) == 0 {
		return nil
	}

	// split continuous event logs into groups by reverted or not
	var splitLogs [][]*types.SubscriptionLog
	var tmplogs []*types.SubscriptionLog

	for i := range changes.Logs {
		if len(tmplogs) == 0 || tmplogs[0].IsRevertLog() == changes.Logs[i].IsRevertLog() {
			tmplogs = append(tmplogs, changes.Logs[i])
			continue
		}

		splitLogs = append(splitLogs, tmplogs)
		tmplogs = []*types.SubscriptionLog{changes.Logs[i]}
	}

	splitLogs = append(splitLogs, tmplogs)

	// convert split logs to linked lists of filter epoch
	epochLists := make([]*list.List, len(splitLogs))

	for i, logs := range splitLogs {
		epochLists[i] = list.New()

		if logs[0].IsRevertLog() { // reorg log group?
			for j := range logs {
				revertTo := logs[j].ChainReorg.RevertTo.ToInt().Uint64()
				epoch := newCfxFilterEpochWithReorg(revertTo)
				epochLists[i].PushBack(epoch)
			}

			continue
		}

		var bhs []string
		blockLogs := make(map[string][]types.Log)

		for j := range logs {
			blockHash := logs[j].Log.BlockHash.String()

			if _, ok := blockLogs[blockHash]; !ok {
				bhs = append(bhs, blockHash)
			}

			blockLogs[blockHash] = append(blockLogs[blockHash], *logs[j].Log)
		}

		for k := range bhs {
			epoch := newCfxFilterEpochFromLogs(blockLogs[bhs[k]])
			epochLists[i].PushBack(epoch)
		}
	}

	return epochLists
}
