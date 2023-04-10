package virtualfilter

import (
	"math/big"
	"testing"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/assert"
)

func TestParseCfxFilterChanges(t *testing.T) {
	testcase := struct {
		logs              []types.CfxFilterLog
		expectedNumLists  int
		expectedListSizes []int
	}{
		logs: []types.CfxFilterLog{
			{ChainReorg: &types.ChainReorg{RevertTo: _newHexBigFromInt(1)}},
			{Log: &types.Log{EpochNumber: _newHexBigFromInt(2), BlockHash: _newCfxHash("0x22")}},
			{Log: &types.Log{EpochNumber: _newHexBigFromInt(3), BlockHash: _newCfxHash("0x23")}},
			{Log: &types.Log{EpochNumber: _newHexBigFromInt(4), BlockHash: _newCfxHash("0x24")}},
			{ChainReorg: &types.ChainReorg{RevertTo: _newHexBigFromInt(3)}},
			{Log: &types.Log{EpochNumber: _newHexBigFromInt(4), BlockHash: _newCfxHash("0x34")}},
			{Log: &types.Log{EpochNumber: _newHexBigFromInt(4), BlockHash: _newCfxHash("0x35")}},
			{Log: &types.Log{EpochNumber: _newHexBigFromInt(4), BlockHash: _newCfxHash("0x36")}},
			{Log: &types.Log{EpochNumber: _newHexBigFromInt(4), BlockHash: _newCfxHash("0x37")}},
		},
		expectedNumLists:  4,
		expectedListSizes: []int{1, 3, 1, 4},
	}

	filterEpochLists := parseCfxFilterChanges(&types.CfxFilterChanges{Logs: testcase.logs})
	assert.Equal(t, testcase.expectedNumLists, len(filterEpochLists))

	for i, j := 0, 0; i < len(filterEpochLists); i++ {
		ll := filterEpochLists[i]
		assert.Equal(t, testcase.expectedListSizes[i], ll.Len())

		for node := ll.Front(); node != nil; j++ {
			epoch := node.Value.(*cfxFilterEpoch)

			var expectedEpoch *cfxFilterEpoch
			if !epoch.reorged() {
				expectedEpoch = newCfxFilterEpochFromLogs([]types.Log{*testcase.logs[j].Log})
			} else {
				revertTo := testcase.logs[j].ChainReorg.RevertTo.ToInt().Uint64()
				expectedEpoch = newCfxFilterEpochWithReorg(revertTo)
			}

			assert.Equal(t, expectedEpoch, epoch)
			node = node.Next()
		}
	}
}

func TestCfxFilterChain(t *testing.T) {
	fchain := newCfxFilterChain(10)
	assert.Nil(t, fchain.front())
	assert.Nil(t, fchain.back())
	assert.Equal(t, 0, fchain.len)

	// test case #1 extend the chain
	demolog := []types.CfxFilterLog{
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(1), BlockHash: _newCfxHash("0x11")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(2), BlockHash: _newCfxHash("0x12")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(3), BlockHash: _newCfxHash("0x13")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(4), BlockHash: _newCfxHash("0x14")}},
		{ChainReorg: &types.ChainReorg{RevertTo: _newHexBigFromInt(3)}},
	}
	filterBlockLists := parseCfxFilterChanges(&types.CfxFilterChanges{Logs: demolog})

	err := fchain.extend(filterBlockLists[0])
	assert.NoError(t, err, "failed to extend filter chain")

	assert.Equal(t, 4, fchain.len)

	assert.NotNil(t, fchain.front())
	assert.NotNil(t, fchain.back())

	assert.True(t, fchain.front().cursor() == filterCursor{
		height: demolog[0].Log.EpochNumber.ToInt().Uint64(),
		hash:   demolog[0].Log.BlockHash.String(),
	})

	assert.True(t, fchain.back().cursor() == filterCursor{
		height: demolog[3].Log.EpochNumber.ToInt().Uint64(),
		hash:   demolog[3].Log.BlockHash.String(),
	})

	fchain.print(nilFilterCursor)

	i := 0
	fchain.traverse(nilFilterCursor, func(node *filterNode, forkPoint bool) bool {
		assert.True(t, node.cursor() == filterCursor{
			height: demolog[i].Log.EpochNumber.ToInt().Uint64(),
			hash:   demolog[i].Log.BlockHash.String(),
		})

		i++
		return true
	})

	// test case #2 reorg the chain
	err = fchain.reorg(filterBlockLists[1])
	assert.NoError(t, err, "failed to reorg filter chain")

	assert.Equal(t, 3, fchain.len)

	assert.NotNil(t, fchain.front())
	assert.NotNil(t, fchain.back())

	assert.True(t, fchain.front().cursor() == filterCursor{
		height: demolog[0].Log.EpochNumber.ToInt().Uint64(),
		hash:   demolog[0].Log.BlockHash.String(),
	})
	assert.True(t, fchain.back().cursor() == filterCursor{
		height: demolog[2].Log.EpochNumber.ToInt().Uint64(),
		hash:   demolog[2].Log.BlockHash.String(),
	})

	// traverse the canonical chain
	fchain.print(nilFilterCursor)

	fchain.traverse(nilFilterCursor, func() filterChainIterator {
		i := 0
		return func(node *filterNode, forkPoint bool) bool {
			assert.True(t, node.cursor() == filterCursor{
				height: demolog[i].Log.EpochNumber.ToInt().Uint64(),
				hash:   demolog[i].Log.BlockHash.String(),
			})

			i++
			return true
		}
	}())

	// traverse the fork chain
	cursor := filterCursor{
		height: demolog[3].Log.EpochNumber.ToInt().Uint64(),
		hash:   demolog[3].Log.BlockHash.String(),
	}
	fchain.print(cursor)

	fchain.traverse(cursor, func() filterChainIterator {
		i := 3
		return func(node *filterNode, forkPoint bool) bool {
			assert.True(t, node.cursor() == filterCursor{
				height: demolog[i].Log.EpochNumber.ToInt().Uint64(),
				hash:   demolog[i].Log.BlockHash.String(),
			})

			if i == 3 {
				assert.EqualValues(t, node.filterNodable.(*cfxFilterEpoch).revertedTo, 3)
			}

			i--
			return true
		}
	}())

	// test case #3 extend the chain to evict full filter blocks
	demolog2 := []types.CfxFilterLog{
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(4), BlockHash: _newCfxHash("0x24")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(5), BlockHash: _newCfxHash("0x25")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(6), BlockHash: _newCfxHash("0x26")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(7), BlockHash: _newCfxHash("0x27")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(8), BlockHash: _newCfxHash("0x28")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(9), BlockHash: _newCfxHash("0x29")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(10), BlockHash: _newCfxHash("0x30")}},
	}

	filterBlockLists = parseCfxFilterChanges(&types.CfxFilterChanges{Logs: demolog2})

	headEpoch := fchain.front().filterNodable.(*cfxFilterEpoch)
	assert.Positive(t, len(headEpoch.logs))

	err = fchain.extend(filterBlockLists[0])
	assert.NoError(t, err, "failed to extend filter chain")
	assert.Equal(t, 10, fchain.len)

	headEpoch = fchain.front().filterNodable.(*cfxFilterEpoch)
	assert.Zero(t, len(headEpoch.logs))

	nextHeadEpoch := fchain.front().getNext().filterNodable.(*cfxFilterEpoch)
	assert.Positive(t, len(nextHeadEpoch.logs))

	tailEpoch := fchain.back().filterNodable.(*cfxFilterEpoch)
	assert.Positive(t, len(tailEpoch.logs))

	// test case #4 extend the chain with re-orged node
	demolog3 := []types.CfxFilterLog{
		{ChainReorg: &types.ChainReorg{RevertTo: _newHexBigFromInt(8)}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(9), BlockHash: _newCfxHash("0x39")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(10), BlockHash: _newCfxHash("0x40")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(11), BlockHash: _newCfxHash("0x41")}},
		{ChainReorg: &types.ChainReorg{RevertTo: _newHexBigFromInt(8)}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(9), BlockHash: _newCfxHash("0x29")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(10), BlockHash: _newCfxHash("0x30")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(11), BlockHash: _newCfxHash("0x31")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(12), BlockHash: _newCfxHash("0x32")}},
	}

	err = fchain.merge(&types.CfxFilterChanges{Logs: demolog3})
	assert.NoError(t, err, "failed to extend filter chain after reorg")

	fchain.print(fchain.front().cursor())

	node, ok := fchain.hashToNodes[_newCfxHash("0x29").String()]
	assert.True(t, ok && !node.reorged())

	node, ok = fchain.hashToNodes[_newCfxHash("0x30").String()]
	assert.True(t, ok && !node.reorged())

	assert.True(t, fchain.back().cursor() == filterCursor{
		height: 12, hash: _newCfxHash("0x32").String(),
	})
}

func TestCfxFilterChainInitialReorg(t *testing.T) {
	fchain := newCfxFilterChain(10)
	assert.Nil(t, fchain.front())
	assert.Nil(t, fchain.back())
	assert.Equal(t, 0, fchain.len)

	demolog := []types.CfxFilterLog{
		{ChainReorg: &types.ChainReorg{RevertTo: _newHexBigFromInt(3)}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(4), BlockHash: _newCfxHash("0x24")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(5), BlockHash: _newCfxHash("0x25")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(6), BlockHash: _newCfxHash("0x26")}},
		{Log: &types.Log{EpochNumber: _newHexBigFromInt(7), BlockHash: _newCfxHash("0x27")}},
	}

	err := fchain.merge(&types.CfxFilterChanges{Logs: demolog})
	assert.NoError(t, err, "failed to merge filter chain")

	assert.Equal(t, 4, fchain.len)
	assert.NotNil(t, fchain.front())
	assert.NotNil(t, fchain.back())

	assert.True(t, fchain.front().cursor() == filterCursor{
		height: demolog[1].Log.EpochNumber.ToInt().Uint64(),
		hash:   demolog[1].Log.BlockHash.String(),
	})

	assert.True(t, fchain.back().cursor() == filterCursor{
		height: demolog[4].Log.EpochNumber.ToInt().Uint64(),
		hash:   demolog[4].Log.BlockHash.String(),
	})

	fchain.print(nilFilterCursor)

	i := 0
	fchain.traverse(nilFilterCursor, func(node *filterNode, forkPoint bool) bool {
		if node == nil {
			return true
		}

		if i > 0 {
			assert.True(t, node.cursor() == filterCursor{
				height: demolog[i].Log.EpochNumber.ToInt().Uint64(),
				hash:   demolog[i].Log.BlockHash.String(),
			})
		} else {
			assert.EqualValues(t, node.filterNodable.(*cfxFilterEpoch).revertedTo, 3)
		}

		i++
		return true
	})
}

func _newHexBigFromInt(v int64) *hexutil.Big {
	return (*hexutil.Big)(big.NewInt(v))
}

func _newCfxHash(h string) *types.Hash {
	v := types.Hash(h)
	return &v
}
