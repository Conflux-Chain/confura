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
	assert.Equal(t, (*FilterNode)(nil), fchain.Front())
	assert.Equal(t, (*FilterNode)(nil), fchain.Back())
	assert.Equal(t, 0, fchain.len)

	// test case #1 - reorg the chain
	demoLogs1 := []types.Log{
		{BlockNumber: 2, BlockHash: common.HexToHash("0x12"), Removed: true},
		{BlockNumber: 1, BlockHash: common.HexToHash("0x11"), Removed: true},
	}

	fblists := parseFilterChanges(&types.FilterChanges{Logs: demoLogs1})
	err := fchain.Reorg(fblists[0])
	assert.NoError(t, err, "failed to reorg filter chain")

	assert.Equal(t, 0, fchain.len)
	assert.Nil(t, fchain.Front())
	assert.Nil(t, fchain.Back())
	assert.True(t, fchain.exists(demoLogs1[0].BlockHash))
	assert.True(t, fchain.exists(demoLogs1[1].BlockHash))

	cursor1 := FilterCursor{
		blockNum: demoLogs1[0].BlockNumber, blockHash: demoLogs1[0].BlockHash,
	}

	i, expectedTraversalNodes := 0, []*FilterNode{
		{FilterBlock: NewFilterBlockFromLog(&demoLogs1[0])},
		{FilterBlock: NewFilterBlockFromLog(&demoLogs1[1])},
		nil,
	}

	fchain.print(cursor1)

	err = fchain.Traverse(cursor1, func(node *FilterNode, forkPoint bool) bool {
		if node == nil {
			assert.Equal(t, expectedTraversalNodes[i], node)
		} else {
			assert.True(t, expectedTraversalNodes[i].Match(node.FilterBlock))
		}

		i++
		return true
	})
	assert.NoError(t, err, "failed to traverse filter chain")

	// test case #2 - extend the chain
	demoLogs2 := []types.Log{
		{BlockNumber: 1, BlockHash: common.HexToHash("0x21")},
		{BlockNumber: 2, BlockHash: common.HexToHash("0x22")},
		{BlockNumber: 3, BlockHash: common.HexToHash("0x23")},
		{BlockNumber: 4, BlockHash: common.HexToHash("0x24")},
	}

	fblists = parseFilterChanges(&types.FilterChanges{Logs: demoLogs2})
	err = fchain.Extend(fblists[0])
	assert.NoError(t, err, "failed to extend filter chain")

	assert.Equal(t, len(demoLogs2), fchain.len)
	assert.True(t, fchain.Front().Match(NewFilterBlockFromLog(&demoLogs2[0])))
	assert.True(t, fchain.Back().Match(NewFilterBlockFromLog(&demoLogs2[len(demoLogs2)-1])))

	cursor2 := FilterCursor{
		blockNum: demoLogs2[0].BlockNumber, blockHash: demoLogs2[0].BlockHash,
	}
	fchain.print(cursor2)

	// test case #3 - reorg && extend the chain
	demoLogs3 := []types.Log{
		{BlockNumber: 4, BlockHash: common.HexToHash("0x24"), Removed: true},
		{BlockNumber: 3, BlockHash: common.HexToHash("0x23"), Removed: true},
	}

	fblists = parseFilterChanges(&types.FilterChanges{Logs: demoLogs3})
	err = fchain.Reorg(fblists[0])
	assert.NoError(t, err, "failed to reorg filter chain")

	cursor3 := FilterCursor{
		blockNum: demoLogs3[1].BlockNumber, blockHash: demoLogs3[1].BlockHash,
	}
	fchain.print(cursor3)

	demoLogs4 := []types.Log{
		{BlockNumber: 3, BlockHash: common.HexToHash("0x33")},
		{BlockNumber: 4, BlockHash: common.HexToHash("0x34")},
		{BlockNumber: 5, BlockHash: common.HexToHash("0x35")},
		{BlockNumber: 6, BlockHash: common.HexToHash("0x36")},
	}

	fblists = parseFilterChanges(&types.FilterChanges{Logs: demoLogs4})
	err = fchain.Extend(fblists[0])
	assert.NoError(t, err, "failed to extend filter chain")

	fchain.print(cursor3)
}
