package virtualfilter

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/assert"
)

func TestParseEthFilterChanges(t *testing.T) {
	testcase := struct {
		logs              []types.Log
		expectedNumLists  int
		expectedListSizes []int
	}{
		logs: []types.Log{
			{BlockNumber: 2, BlockHash: common.HexToHash("0x12"), Removed: true},
			{BlockNumber: 1, BlockHash: common.HexToHash("0x11"), Removed: true},
			{BlockNumber: 1, BlockHash: common.HexToHash("0x21")},
			{BlockNumber: 2, BlockHash: common.HexToHash("0x22")},
			{BlockNumber: 3, BlockHash: common.HexToHash("0x23")},
			{BlockNumber: 4, BlockHash: common.HexToHash("0x24")},
			{BlockNumber: 4, BlockHash: common.HexToHash("0x24"), Removed: true},
			{BlockNumber: 3, BlockHash: common.HexToHash("0x23"), Removed: true},
			{BlockNumber: 3, BlockHash: common.HexToHash("0x33")},
			{BlockNumber: 4, BlockHash: common.HexToHash("0x34")},
			{BlockNumber: 5, BlockHash: common.HexToHash("0x35")},
			{BlockNumber: 6, BlockHash: common.HexToHash("0x36")},
		},
		expectedNumLists:  4,
		expectedListSizes: []int{2, 4, 2, 4},
	}

	filterBlockLists := parseEthFilterChanges(&types.FilterChanges{Logs: testcase.logs})
	assert.Equal(t, testcase.expectedNumLists, len(filterBlockLists))

	for i, j := 0, 0; i < len(filterBlockLists); i++ {
		ll := filterBlockLists[i]
		assert.Equal(t, testcase.expectedListSizes[i], ll.Len())

		for node := ll.Front(); node != nil; j++ {
			block := node.Value.(*ethFilterBlock)

			expectedBlock := newEthFilterBlockFromLogs(testcase.logs[j : j+1])
			assert.Equal(t, expectedBlock, block)

			node = node.Next()
		}
	}
}

func TestEthFilterChain(t *testing.T) {
	fchain := newEthFilterChain(10)
	assert.Nil(t, fchain.front())
	assert.Nil(t, fchain.back())
	assert.Equal(t, 0, fchain.len)

	demolog := []types.Log{
		{BlockNumber: 1, BlockHash: common.HexToHash("0x11")},
		{BlockNumber: 2, BlockHash: common.HexToHash("0x12")},
		{BlockNumber: 3, BlockHash: common.HexToHash("0x13")},
		{BlockNumber: 4, BlockHash: common.HexToHash("0x14")},
		{BlockNumber: 4, BlockHash: common.HexToHash("0x14"), Removed: true},
	}
	filterBlockLists := parseEthFilterChanges(&types.FilterChanges{Logs: demolog})

	// test case #1 extend the chain
	err := fchain.extend(filterBlockLists[0])
	assert.NoError(t, err, "failed to extend filter chain")
	assert.Equal(t, 4, fchain.len)
	assert.NotNil(t, fchain.front())
	assert.NotNil(t, fchain.back())
	assert.True(t, fchain.front().pointedAt(newEthFilterBlockFromLogs(demolog[0:1])))
	assert.True(t, fchain.back().pointedAt(newEthFilterBlockFromLogs(demolog[3:4])))

	printFilterChain(fchain, nil)

	i := 0
	fchain.traverse(nil, func(node *filterNode, forkPoint bool) bool {
		assert.True(t, node.pointedAt(newEthFilterBlockFromLogs(demolog[i:i+1])))

		i++
		return true
	})

	// test case #2 reorg the chain
	err = fchain.reorg(filterBlockLists[1])
	assert.NoError(t, err, "failed to reorg filter chain")
	assert.Equal(t, 3, fchain.len)
	assert.NotNil(t, fchain.front())
	assert.NotNil(t, fchain.back())
	assert.True(t, fchain.front().pointedAt(newEthFilterBlockFromLogs(demolog[0:1])))
	assert.True(t, fchain.back().pointedAt(newEthFilterBlockFromLogs(demolog[2:3])))

	// traverse the canonical chain
	printFilterChain(fchain, nil)

	fchain.traverse(nil, func() filterChainIterator {
		i := 0
		return func(node *filterNode, forkPoint bool) bool {
			assert.True(t, node.pointedAt(newEthFilterBlockFromLogs(demolog[i:i+1])))
			i++

			return true
		}
	}())

	// traverse the fork chain
	cursor := ethFilterCursor{
		blockNum: demolog[4].BlockNumber, blockHash: demolog[4].BlockHash,
	}
	printFilterChain(fchain, cursor)

	fchain.traverse(cursor, func() filterChainIterator {
		i := 3
		return func(node *filterNode, forkPoint bool) bool {
			assert.True(t, node.pointedAt(newEthFilterBlockFromLogs(demolog[i:i+1])))
			i--
			return true
		}
	}())

	// test case #3 extend the chain to evict full filter blocks
	demolog2 := []types.Log{
		{BlockNumber: 4, BlockHash: common.HexToHash("0x24")},
		{BlockNumber: 5, BlockHash: common.HexToHash("0x25")},
		{BlockNumber: 6, BlockHash: common.HexToHash("0x26")},
		{BlockNumber: 7, BlockHash: common.HexToHash("0x27")},
		{BlockNumber: 8, BlockHash: common.HexToHash("0x28")},
		{BlockNumber: 9, BlockHash: common.HexToHash("0x29")},
		{BlockNumber: 10, BlockHash: common.HexToHash("0x30")},
	}
	filterBlockLists = parseEthFilterChanges(&types.FilterChanges{Logs: demolog2})

	headBlock := fchain.front().filterCursor.(*ethFilterBlock)
	assert.Positive(t, len(headBlock.logs))

	err = fchain.extend(filterBlockLists[0])
	assert.NoError(t, err, "failed to extend filter chain")
	assert.Equal(t, 10, fchain.len)

	headBlock = fchain.front().filterCursor.(*ethFilterBlock)
	assert.Zero(t, len(headBlock.logs))

	nextHeadBlock := fchain.front().getNext().filterCursor.(*ethFilterBlock)
	assert.Positive(t, len(nextHeadBlock.logs))

	tailBlock := fchain.back().filterCursor.(*ethFilterBlock)
	assert.Positive(t, len(tailBlock.logs))
}
