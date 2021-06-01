package sync

import (
	"github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
)

// epochWindow maintains a continuous epoch window with a fixed size capacity.
type epochWindow struct {
	capacity           uint32
	epochFrom, epochTo uint64
}

func newEpochWindow(capacity uint32) *epochWindow {
	return &epochWindow{
		capacity:  capacity,
		epochFrom: types.EpochNumberNil,
		epochTo:   types.EpochNumberNil,
	}
}

// Reset epochFrom and epochTo of the epoch window to some specified epoch number.
func (win *epochWindow) reset(epochFrom, epochTo uint64) {
	win.epochFrom, win.epochTo = epochFrom, epochTo
}

// Expand the epoch window from a smaller epoch number.
func (win *epochWindow) expandFrom(epochNo uint64) {
	if win.isSet() {
		win.epochFrom = util.MinUint64(win.epochFrom, epochNo)
	} else {
		win.reset(epochNo, epochNo)
	}
}

// Expand the epoch window to a bigger epoch number.
func (win *epochWindow) expandTo(epochNo uint64) {
	if win.isSet() {
		win.epochTo = util.MaxUint64(win.epochTo, epochNo)
	} else {
		win.reset(epochNo, epochNo)
	}
}

// Peek if pivot switch will happen if new epoch appended to expand.
func (win *epochWindow) peekWillPivotSwitch(epochNo uint64) bool {
	if win.isSet() && win.epochFrom > epochNo {
		return true
	}

	return false
}

// Peek if overflow will happen if new epoch appended to expand.
func (win *epochWindow) peekWillOverflow(epochNo uint64) bool {
	if win.isSet() && epochNo >= win.epochFrom && (epochNo-win.epochFrom+1) > uint64(win.capacity) {
		return true
	}

	return false
}

// Peek sync info by shrinking no more than the specified size of epoch(s) from the epoch window.
func (win *epochWindow) peekShrinkFrom(specSize uint32) (syncFrom uint64, syncSize uint32) {
	if win.isEmpty() {
		return 0, 0
	}

	return win.epochFrom, util.MinUint32(win.size(), specSize)
}

// Shrink no more than the specified size of epoch(s) from the epoch window.
func (win *epochWindow) shrinkFrom(specSize uint32) (uint64, uint32) {
	if win.isEmpty() {
		return 0, 0
	}

	syncFrom, syncSize := win.epochFrom, util.MinUint32(win.size(), specSize)
	win.epochFrom += uint64(syncSize)

	return syncFrom, syncSize
}

// Check if the epoch window is empty.
func (win *epochWindow) isEmpty() bool {
	return !win.isSet() || win.epochFrom > win.epochTo
}

// Check if the epoch window is set.
func (win *epochWindow) isSet() bool {
	return win.epochFrom != types.EpochNumberNil && win.epochTo != types.EpochNumberNil
}

// Return the size of the sync window.
func (win *epochWindow) size() uint32 {
	if !win.isSet() || win.epochFrom > win.epochTo {
		return 0
	}

	return uint32(win.epochTo - win.epochFrom + 1)
}
