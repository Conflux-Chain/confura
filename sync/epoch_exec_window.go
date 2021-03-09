package sync

import (
	"fmt"
	"math/big"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

const epochGapStateMined = 4

// EpochExecWindow records the recent executed epochs.
type EpochExecWindow struct {
	minEpoch        uint64
	maxEpoch        uint64
	everExecuted    bool
	executedEpochCh chan *big.Int
}

// NewEpochExecWindow creates an instance of EpochExecWindow to track executed epochs.
func NewEpochExecWindow() *EpochExecWindow {
	return &EpochExecWindow{
		executedEpochCh: make(chan *big.Int),
	}
}

// Executed returns the channel for executed epoch.
func (window *EpochExecWindow) Executed() <-chan *big.Int {
	return window.executedEpochCh
}

// Diff differeniate executed epoch against last state epoch.
func (window *EpochExecWindow) Diff(cfx sdk.ClientOperator) {
	var lastExecutedEpoch int64

	for epoch := range window.executedEpochCh {
		epochNum := epoch.Int64()
		fmt.Printf("[EXECUTED EPOCH] %v", epochNum)

		if statedEpoch, err := cfx.GetEpochNumber(types.EpochLatestState); err == nil {
			if delta := epochNum - statedEpoch.ToInt().Int64(); delta != 0 {
				fmt.Printf(" (gap %v)", delta)
			}
		}

		if lastExecutedEpoch > 0 {
			if delta := epochNum - lastExecutedEpoch; delta != 1 {
				fmt.Printf(" (reverted %v)", delta)
			}
		}

		lastExecutedEpoch = epochNum

		fmt.Println()
	}
}

func (window *EpochExecWindow) onEpochReceived(epoch types.WebsocketEpochResponse) {
	epochNum := epoch.EpochNumber.ToInt().Uint64()
	if window.update(epochNum) {
		window.executedEpochCh <- new(big.Int).SetUint64(window.minEpoch)
	}
}

func (window *EpochExecWindow) update(epoch uint64) bool {
	if epoch == 0 {
		panic("cannot push epoch 0")
	}

	// empty window
	if window.minEpoch == 0 {
		window.minEpoch = epoch
		window.maxEpoch = epoch
		return false
	}

	// epoch missed
	if epoch > window.maxEpoch+1 {
		panic(fmt.Sprintf("new epoch too large, current = %v, new = %v", window.maxEpoch, epoch))
	}

	// even stated epochs are reverted
	if epoch <= window.minEpoch {
		window.minEpoch = epoch
		window.maxEpoch = epoch
		return window.everExecuted
	}

	// pop in case of pivot chain switched
	if epoch <= window.maxEpoch {
		window.maxEpoch = epoch
		return false
	}

	// push new epoch
	window.maxEpoch = epoch

	if window.maxEpoch-window.minEpoch < epochGapStateMined {
		return false
	}

	if window.maxEpoch-window.minEpoch > epochGapStateMined {
		window.minEpoch++
		window.everExecuted = true
		return true
	}

	if window.everExecuted {
		return false
	}

	window.everExecuted = true
	return true
}
