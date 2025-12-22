package sync

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockHash string

type mockBlock struct {
	number     uint64
	hash       mockHash
	parentHash mockHash
}

func (b mockBlock) Number() uint64       { return b.number }
func (b mockBlock) Hash() mockHash       { return b.hash }
func (b mockBlock) ParentHash() mockHash { return b.parentHash }

func createChain(start uint64, count int) []mockBlock {
	blocks := make([]mockBlock, count)
	for i := range count {
		num := start + uint64(i)
		var parentHash mockHash
		if i == 0 {
			parentHash = mockHash("genesis")
		} else {
			parentHash = blocks[i-1].hash
		}
		blocks[i] = mockBlock{
			number:     num,
			hash:       mockHash(fmt.Sprintf("hash_%d", num)),
			parentHash: parentHash,
		}
	}
	return blocks
}

func TestNewSlidingPivotWindow(t *testing.T) {
	t.Run("valid capacity", func(t *testing.T) {
		w := newSlidingPivotWindow[mockBlock](10)
		assert.NotNil(t, w)
		assert.Equal(t, uint32(0), w.Size())
	})

	t.Run("zero capacity panics", func(t *testing.T) {
		assert.Panics(t, func() {
			newSlidingPivotWindow[mockBlock](0)
		})
	})
}

func TestSlidingPivotWindowPushAndGet(t *testing.T) {
	w := newSlidingPivotWindow[mockBlock](5)
	blocks := createChain(100, 3)

	// push 3 blocks
	for _, b := range blocks {
		err := w.Push(b)
		require.NoError(t, err)
	}

	assert.Equal(t, uint32(3), w.Size())

	// valid gets
	for _, b := range blocks {
		hash, found := w.Get(b.number)
		assert.True(t, found)
		assert.Equal(t, b.hash, hash)
	}

	// invalid gets
	_, found := w.Get(99)
	assert.False(t, found)

	_, found = w.Get(103)
	assert.False(t, found)
}

func TestSlidingPivotWindowEmptyGet(t *testing.T) {
	w := newSlidingPivotWindow[mockBlock](5)

	_, found := w.Get(100)
	assert.False(t, found)
}

func TestSlidingPivotWindowEviction(t *testing.T) {
	capacity := uint32(3)
	w := newSlidingPivotWindow[mockBlock](capacity)
	blocks := createChain(100, 5)

	// push 5 blocks
	for _, b := range blocks {
		err := w.Push(b)
		require.NoError(t, err)
	}

	assert.Equal(t, capacity, w.Size())

	// validate eviction
	_, found := w.Get(100)
	assert.False(t, found, "block 100 should be evicted")

	_, found = w.Get(101)
	assert.False(t, found, "block 101 should be evicted")

	for i := 2; i < 5; i++ {
		hash, found := w.Get(blocks[i].number)
		assert.True(t, found)
		assert.Equal(t, blocks[i].hash, hash)
	}
}

func TestSlidingPivotWindowFullCapacityWrap(t *testing.T) {
	capacity := uint32(3)
	w := newSlidingPivotWindow[mockBlock](capacity)
	blocks := createChain(0, 10)

	for _, b := range blocks {
		err := w.Push(b)
		require.NoError(t, err)
	}

	assert.Equal(t, capacity, w.Size())

	for i := range 7 {
		_, found := w.Get(uint64(i))
		assert.False(t, found)
	}

	for i := 7; i < 10; i++ {
		hash, found := w.Get(uint64(i))
		assert.True(t, found)
		assert.Equal(t, blocks[i].hash, hash)
	}
}

func TestSlidingPivotWindowPushNonContinuousNumber(t *testing.T) {
	w := newSlidingPivotWindow[mockBlock](5)

	block1 := mockBlock{number: 100, hash: "h100", parentHash: "genesis"}
	err := w.Push(block1)
	require.NoError(t, err)

	block3 := mockBlock{number: 102, hash: "h102", parentHash: "h100"}
	err = w.Push(block3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not continuous")

	assert.Equal(t, uint32(1), w.Size())
}

func TestSlidingPivotWindowPushParentHashMismatch(t *testing.T) {
	w := newSlidingPivotWindow[mockBlock](5)

	block1 := mockBlock{number: 100, hash: "h100", parentHash: "genesis"}
	err := w.Push(block1)
	require.NoError(t, err)

	block2 := mockBlock{number: 101, hash: "h101", parentHash: "wrong_parent"}
	err = w.Push(block2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parent hash mismatch")

	assert.Equal(t, uint32(1), w.Size())
}

func TestSlidingPivotWindowPopTo(t *testing.T) {
	tests := []struct {
		name          string
		initialBlocks int
		startNum      uint64
		popToTarget   uint64
		expectedSize  uint32
		remainingNums []uint64
	}{
		{
			name:          "pop some from end",
			initialBlocks: 5,
			startNum:      100,
			popToTarget:   103,
			expectedSize:  3,
			remainingNums: []uint64{100, 101, 102},
		},
		{
			name:          "pop all",
			initialBlocks: 5,
			startNum:      100,
			popToTarget:   100,
			expectedSize:  0,
			remainingNums: []uint64{},
		},
		{
			name:          "pop to before start clears all",
			initialBlocks: 5,
			startNum:      100,
			popToTarget:   50,
			expectedSize:  0,
			remainingNums: []uint64{},
		},
		{
			name:          "pop to after end does nothing",
			initialBlocks: 5,
			startNum:      100,
			popToTarget:   200,
			expectedSize:  5,
			remainingNums: []uint64{100, 101, 102, 103, 104},
		},
		{
			name:          "pop to exactly end+1 does nothing",
			initialBlocks: 5,
			startNum:      100,
			popToTarget:   105,
			expectedSize:  5,
			remainingNums: []uint64{100, 101, 102, 103, 104},
		},
		{
			name:          "pop last one",
			initialBlocks: 5,
			startNum:      100,
			popToTarget:   104,
			expectedSize:  4,
			remainingNums: []uint64{100, 101, 102, 103},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newSlidingPivotWindow[mockBlock](10)
			blocks := createChain(tt.startNum, tt.initialBlocks)

			for _, b := range blocks {
				err := w.Push(b)
				require.NoError(t, err)
			}

			w.PopTo(tt.popToTarget)

			assert.Equal(t, tt.expectedSize, w.Size())

			for _, num := range tt.remainingNums {
				_, found := w.Get(num)
				assert.True(t, found, "block %d should exist", num)
			}

			for i := tt.popToTarget; i < tt.startNum+uint64(tt.initialBlocks); i++ {
				_, found := w.Get(i)
				assert.False(t, found, "block %d should be popped", i)
			}
		})
	}
}

func TestSlidingPivotWindowPopToEmpty(t *testing.T) {
	w := newSlidingPivotWindow[mockBlock](5)

	w.PopTo(100)
	assert.Equal(t, uint32(0), w.Size())
}

func TestSlidingPivotWindowReset(t *testing.T) {
	w := newSlidingPivotWindow[mockBlock](5)
	blocks := createChain(100, 3)

	for _, b := range blocks {
		err := w.Push(b)
		require.NoError(t, err)
	}

	assert.Equal(t, uint32(3), w.Size())

	w.Reset()

	assert.Equal(t, uint32(0), w.Size())

	for _, b := range blocks {
		_, found := w.Get(b.number)
		assert.False(t, found)
	}

	newBlocks := createChain(500, 2)
	for _, b := range newBlocks {
		err := w.Push(b)
		require.NoError(t, err)
	}

	assert.Equal(t, uint32(2), w.Size())
	hash, found := w.Get(500)
	assert.True(t, found)
	assert.Equal(t, newBlocks[0].hash, hash)
}

func TestSlidingPivotWindowConcurrent(t *testing.T) {
	w := newSlidingPivotWindow[mockBlock](100)
	blocks := createChain(0, 1000)

	for i := range 50 {
		err := w.Push(blocks[i])
		require.NoError(t, err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 50; i < 1000; i++ {
			_ = w.Push(blocks[i])
		}
	}()

	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 10000 {
				w.Get(uint64(j % 1000))
				w.Size()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			w.PopTo(uint64(i * 10))
		}
	}()

	wg.Wait()
}

func TestSlidingPivotWindowSingleCapacity(t *testing.T) {
	w := newSlidingPivotWindow[mockBlock](1)
	blocks := createChain(100, 5)

	for i, b := range blocks {
		err := w.Push(b)
		require.NoError(t, err)

		assert.Equal(t, uint32(1), w.Size())

		hash, found := w.Get(b.number)
		assert.True(t, found)
		assert.Equal(t, b.hash, hash)

		if i > 0 {
			_, found := w.Get(blocks[i-1].number)
			assert.False(t, found)
		}
	}
}

func TestSlidingPivotWindowLargeNumbers(t *testing.T) {
	w := newSlidingPivotWindow[mockBlock](5)

	start := uint64(1<<63 - 10)
	blocks := createChain(start, 5)

	for _, b := range blocks {
		err := w.Push(b)
		require.NoError(t, err)
	}

	assert.Equal(t, uint32(5), w.Size())

	for _, b := range blocks {
		hash, found := w.Get(b.number)
		assert.True(t, found)
		assert.Equal(t, b.hash, hash)
	}
}

func TestSlidingPivotWindowPushAfterPopTo(t *testing.T) {
	w := newSlidingPivotWindow[mockBlock](10)

	blocks := createChain(100, 5)
	for _, b := range blocks {
		err := w.Push(b)
		require.NoError(t, err)
	}

	w.PopTo(103)
	assert.Equal(t, uint32(3), w.Size())

	newBlock := mockBlock{
		number:     103,
		hash:       "new_h103",
		parentHash: blocks[2].hash, // h102
	}
	err := w.Push(newBlock)
	require.NoError(t, err)

	assert.Equal(t, uint32(4), w.Size())

	hash, found := w.Get(103)
	assert.True(t, found)
	assert.Equal(t, mockHash("new_h103"), hash)
}
