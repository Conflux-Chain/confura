package sync

import (
	"sync"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	w3ctypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

type Chainable[H comparable] interface {
	Number() uint64
	Hash() H
	ParentHash() H
}

type slidingPivotWindow[T Chainable[H], H comparable] struct {
	mu sync.RWMutex

	pivots []H    // pivot hash circular buffer
	head   uint32 // array index for the oldest pivot hash

	length   uint32 // number of pivot hashes
	capacity uint32 // maximum number of pivot hashes

	start uint64 // starting block number
}

func newSlidingPivotWindow[T Chainable[H], H comparable](capacity uint32) *slidingPivotWindow[T, H] {
	if capacity == 0 {
		panic("capacity must be positive")
	}

	return &slidingPivotWindow[T, H]{
		pivots:   make([]H, capacity),
		capacity: capacity,
	}
}

func (w *slidingPivotWindow[T, H]) Get(number uint64) (hash H, found bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.length == 0 {
		return
	}

	if number < w.start || number >= w.start+uint64(w.length) {
		return
	}

	offset := uint32(number - w.start)
	idx := (w.head + offset) % w.capacity
	return w.pivots[idx], true
}

func (w *slidingPivotWindow[T, H]) Push(node T) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.length > 0 {
		expectedNumber := w.start + uint64(w.length)

		// validate block number
		if seq := node.Number(); seq != expectedNumber {
			return errors.Errorf("seq not continuous, expect %v got %v", expectedNumber, seq)
		}

		// validate parent hash
		lastIdx := (w.head + w.length - 1) % w.capacity
		if ph := node.ParentHash(); ph != w.pivots[lastIdx] {
			return errors.Errorf("parent hash mismatch, expect %v got %v", w.pivots[lastIdx], ph)
		}

		// evict the oldest pivot if the window is full
		if w.length == w.capacity {
			w.head = (w.head + 1) % w.capacity
			w.start++
			w.length--
		}
	} else {
		w.start = node.Number()
	}

	// write the new pivot
	writeIdx := (w.head + w.length) % w.capacity
	w.pivots[writeIdx] = node.Hash()
	w.length++

	return nil
}

// PopTo removes all entries with number >= target (i.e. target is exclusive).
// Used for chain reorg/rollback. After PopTo, valid range is [start, target).
func (w *slidingPivotWindow[T, H]) PopTo(target uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.length == 0 {
		return
	}

	end := w.start + uint64(w.length) - 1
	if target > end {
		return
	}

	if target <= w.start {
		w.length = 0
		return
	}

	newLen := target - w.start // target is exclusive
	w.length = uint32(newLen)
}

func (w *slidingPivotWindow[T, H]) Size() uint32 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.length
}

func (w *slidingPivotWindow[T, H]) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.head, w.length = 0, 0
}

type cfxPivotBlockWrapper struct {
	*types.Block
}

func (b cfxPivotBlockWrapper) Number() uint64         { return b.EpochNumber.ToInt().Uint64() }
func (b cfxPivotBlockWrapper) Hash() types.Hash       { return b.Block.Hash }
func (b cfxPivotBlockWrapper) ParentHash() types.Hash { return b.Block.ParentHash }

type cfxSlidingPivotWindow = slidingPivotWindow[cfxPivotBlockWrapper, types.Hash]

func newCfxSlidingPivotWindow(capacity uint32) *cfxSlidingPivotWindow {
	return newSlidingPivotWindow[cfxPivotBlockWrapper](capacity)
}

type evmPivotBlockWrapper struct {
	*w3ctypes.Block
}

func (b evmPivotBlockWrapper) Number() uint64          { return b.Block.Number.Uint64() }
func (b evmPivotBlockWrapper) Hash() common.Hash       { return b.Block.Hash }
func (b evmPivotBlockWrapper) ParentHash() common.Hash { return b.Block.ParentHash }

type evmSlidingPivotWindow = slidingPivotWindow[evmPivotBlockWrapper, common.Hash]

func newEvmSlidingPivotWindow(capacity uint32) *evmSlidingPivotWindow {
	return newSlidingPivotWindow[evmPivotBlockWrapper](capacity)
}
