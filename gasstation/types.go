package gasstation

import (
	"time"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

type gasPriceUsedStats struct {
	// min, max, median, mean statistics index
	min    float64
	mean   float64
	max    float64
	median float64
}

type gasPriceUsedSample struct {
	tx            *types.Transaction
	epochNo       uint64
	minedDuration time.Duration
}

type gasPriceSamplingWindow struct {
	startEpoch          uint64                             // window start epoch
	endEpoch            uint64                             // window end epoch
	capacity            uint64                             // window capacity
	epochToTxs          map[uint64][]*types.Hash           // epoch => [tx hashes...]
	epochToPivotBlocks  map[uint64]*types.BlockSummary     // epoch => pivot block
	txToEstimateSamples map[types.Hash]*gasPriceUsedSample // tx hash => gas price used sample
}

func newGasPriceSamplingWindow(capacity uint64) *gasPriceSamplingWindow {
	return &gasPriceSamplingWindow{
		capacity:            capacity,
		epochToTxs:          make(map[uint64][]*types.Hash),
		epochToPivotBlocks:  make(map[uint64]*types.BlockSummary),
		txToEstimateSamples: make(map[types.Hash]*gasPriceUsedSample),
	}
}

func (win *gasPriceSamplingWindow) isEmpty() bool {
	// initial set or empty window?
	return win.endEpoch == 0 || (win.startEpoch > win.endEpoch)
}

func (win *gasPriceSamplingWindow) size() uint64 {
	if win.isEmpty() {
		return 0
	}

	return win.endEpoch - win.startEpoch + 1
}

func (win *gasPriceSamplingWindow) expandTo(newEpoch uint64) bool {
	if win.isEmpty() {
		win.startEpoch = newEpoch
		win.endEpoch = newEpoch

		return true
	}

	if win.endEpoch >= newEpoch {
		return false
	}

	win.endEpoch = newEpoch

	for win.size() > win.capacity { // in case of window overflow
		epochNo := win.startEpoch

		txHashes := win.epochToTxs[epochNo]
		delete(win.epochToTxs, epochNo)

		for _, txh := range txHashes {
			delete(win.txToEstimateSamples, *txh)
		}

		delete(win.epochToPivotBlocks, epochNo)

		win.startEpoch++
	}

	return true
}
