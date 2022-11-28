package virtualfilter

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/assert"
)

func TestParseFilterChanges(t *testing.T) {
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
		expectedListSizes: []int{2, 4, 2, 2},
	}

	filterBlockLists := parseFilterChanges(&types.FilterChanges{Logs: testcase.logs})
	assert.Equal(t, testcase.expectedNumLists, len(filterBlockLists))

	for i, j := 0, 0; i < len(filterBlockLists); i++ {
		ll := filterBlockLists[i]
		assert.Equal(t, testcase.expectedListSizes[i], ll.Len())

		for node := ll.Front(); node != nil; j++ {
			block := node.Value.(*FilterBlock)

			expectedBlock := NewFilterBlockFromLog(&testcase.logs[j])
			assert.Equal(t, expectedBlock.FilterCursor, block.FilterCursor)
			assert.Equal(t, expectedBlock.reorg, block.reorg)

			node = node.Next()
		}
	}
}

func TestFilterChain(t *testing.T) {
	fchain := NewFilterChain()
	assert.Nil(t, fchain.Front())
	assert.Nil(t, fchain.Back())
	assert.Equal(t, 0, fchain.len)

	demolog := []types.Log{
		{BlockNumber: 1, BlockHash: common.HexToHash("0x11")},
		{BlockNumber: 2, BlockHash: common.HexToHash("0x12")},
		{BlockNumber: 3, BlockHash: common.HexToHash("0x13")},
		{BlockNumber: 4, BlockHash: common.HexToHash("0x14")},
		{BlockNumber: 4, BlockHash: common.HexToHash("0x14"), Removed: true},
	}
	filterBlockLists := parseFilterChanges(&types.FilterChanges{Logs: demolog})

	// test case #1 extend the chain
	err := fchain.Extend(filterBlockLists[0])
	assert.NoError(t, err, "failed to extend filter chain")
	assert.Equal(t, 4, fchain.len)
	assert.NotNil(t, fchain.Front())
	assert.NotNil(t, fchain.Back())
	assert.True(t, fchain.Front().Match(NewFilterBlockFromLog(&demolog[0])))
	assert.True(t, fchain.Back().Match(NewFilterBlockFromLog(&demolog[3])))

	fchain.print(nilFilterCursor)

	i := 0
	fchain.Traverse(nilFilterCursor, func(node *FilterNode, forkPoint bool) bool {
		assert.True(t, node.Match(NewFilterBlockFromLog(&demolog[i])))

		i++
		return true
	})

	// test case #2 reorg the chain
	err = fchain.Reorg(filterBlockLists[1])
	assert.NoError(t, err, "failed to reorg filter chain")
	assert.Equal(t, 3, fchain.len)
	assert.NotNil(t, fchain.Front())
	assert.NotNil(t, fchain.Back())
	assert.True(t, fchain.Front().Match(NewFilterBlockFromLog(&demolog[0])))
	assert.True(t, fchain.Back().Match(NewFilterBlockFromLog(&demolog[2])))

	// traverse the canonical chain
	fchain.print(nilFilterCursor)

	fchain.Traverse(nilFilterCursor, func() FilterChainIteratorFunc {
		i := 0
		return func(node *FilterNode, forkPoint bool) bool {
			assert.True(t, node.Match(NewFilterBlockFromLog(&demolog[i])))
			i++

			return true
		}
	}())

	// traverse the fork chain
	cursor := FilterCursor{
		blockNum: demolog[4].BlockNumber, blockHash: demolog[4].BlockHash,
	}
	fchain.print(cursor)

	fchain.Traverse(cursor, func() FilterChainIteratorFunc {
		i := 0
		return func(node *FilterNode, forkPoint bool) bool {
			assert.True(t, node.Match(NewFilterBlockFromLog(&demolog[3-i])))
			i++
			return true
		}
	}())
}
