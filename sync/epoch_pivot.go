package sync

import (
	"sync"

	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
)

// epochPivotWindow caches epoch pivot info with limited capacity.
type epochPivotWindow struct {
	mu sync.Mutex

	// hashmap to cache pivot hash of epoch (epoch number => pivot block hash)
	epochToPivotHash map[uint64]types.Hash
	// maximum number of epochs to hold
	capacity uint32
	// cached epoch range
	epochFrom, epochTo uint64
}

func newEpochPivotWindow(capacity uint32) *epochPivotWindow {
	win := &epochPivotWindow{capacity: capacity}
	win.reset()

	return win
}

func (win *epochPivotWindow) GetPivotHash(epoch uint64) (types.Hash, bool) {
	win.mu.Lock()
	defer win.mu.Unlock()

	pivotHash, ok := win.epochToPivotHash[epoch]
	return pivotHash, ok
}

func (win *epochPivotWindow) Reset() {
	win.mu.Lock()
	defer win.mu.Unlock()

	win.reset()
}

func (win *epochPivotWindow) reset() {
	win.epochFrom = citypes.EpochNumberNil
	win.epochTo = citypes.EpochNumberNil

	win.epochToPivotHash = make(map[uint64]types.Hash)
}

func (win *epochPivotWindow) Push(pivotBlock *types.Block) error {
	win.mu.Lock()
	defer win.mu.Unlock()

	pivotEpochNum := pivotBlock.EpochNumber.ToInt().Uint64()

	if win.size() > 0 { // validate incoming pivot block
		if (win.epochTo + 1) != pivotEpochNum {
			return errors.Errorf(
				"incontinuous epoch pushed, expect %v got %v", win.epochTo+1, pivotEpochNum,
			)
		}

		latestPivotHash, ok := win.epochToPivotHash[win.epochTo]
		if !ok || pivotBlock.ParentHash != latestPivotHash {
			return errors.Errorf(
				"mismatched parent hash, expect %v got %v", latestPivotHash, pivotBlock.ParentHash,
			)
		}
	}

	// reclaim in case of memory blast
	for win.size() != 0 && win.size() >= win.capacity {
		delete(win.epochToPivotHash, win.epochFrom)
		win.epochFrom++
	}

	// cache store epoch pivot hash
	win.epochToPivotHash[pivotEpochNum] = pivotBlock.Hash
	win.expandTo(pivotEpochNum)

	return nil
}

func (win *epochPivotWindow) expandTo(newEpoch uint64) {
	if !win.isSet() {
		win.epochFrom, win.epochTo = newEpoch, newEpoch
	} else if win.epochTo < newEpoch {
		win.epochTo = newEpoch
	}
}

func (win *epochPivotWindow) Popn(epochUntil uint64) {
	win.mu.Lock()
	defer win.mu.Unlock()

	if win.size() == 0 || win.epochTo < epochUntil {
		return
	}

	for win.epochTo >= epochUntil {
		delete(win.epochToPivotHash, win.epochTo)
		win.epochTo--

		if win.size() == 0 {
			win.reset()
			return
		}
	}
}

func (win *epochPivotWindow) isSet() bool {
	return win.epochFrom != citypes.EpochNumberNil && win.epochTo != citypes.EpochNumberNil
}

func (win *epochPivotWindow) size() uint32 {
	if !win.isSet() || win.epochFrom > win.epochTo {
		return 0
	}

	return uint32(win.epochTo - win.epochFrom + 1)
}
