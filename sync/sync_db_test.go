package sync

import (
	"math"
	"testing"

	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/stretchr/testify/assert"
)

func TestFindFirstRevertedEpochInRange(t *testing.T) {
	syncer := &DatabaseSyncer{}

	testCases := []struct {
		epochRange    citypes.EpochRange
		firstReverted uint64
		expected      uint64
	}{
		{ // first reverted in middle of epoch range
			epochRange:    citypes.EpochRange{10, 50},
			firstReverted: 20,
			expected:      20,
		},
		{ // first reverted in right edge of epoch range
			epochRange:    citypes.EpochRange{10, 50},
			firstReverted: 50,
			expected:      50,
		},
		{ // first reverted in left edge of epoch range
			epochRange:    citypes.EpochRange{10, 50},
			firstReverted: 1,
			expected:      10,
		},
		{ // first reverted out of right side of epoch range
			epochRange:    citypes.EpochRange{10, 50},
			firstReverted: 51,
			expected:      0,
		},
		{ // first reverted out of left side of epoch range
			epochRange:    citypes.EpochRange{10, 50},
			firstReverted: 1,
			expected:      10,
		},
	}

	for i, tc := range testCases {
		t.Logf(">>>>>> run testcase %v", i+1)

		// Epoch reverted checker
		checker := func(epochNo uint64) (bool, error) {
			t.Logf("check epoch: %v", epochNo)
			if epochNo >= tc.firstReverted {
				return true, nil
			}
			return false, nil
		}
		res, err := syncer.findFirstRevertedEpochInRange(tc.epochRange, checker)
		assert.Nil(t, err)
		assert.Equal(t, tc.expected, res)
	}
}

func TestEnsureEpochRangeNotRerverted(t *testing.T) {
	syncer := &DatabaseSyncer{}

	testCases := []struct {
		epochRange              citypes.EpochRange
		firstReverted           uint64
		expectedPrunedEpochFrom uint64
	}{
		{ // first reverted in middle of epoch range
			epochRange:              citypes.EpochRange{100, 501},
			firstReverted:           200,
			expectedPrunedEpochFrom: 200,
		},
		{ // first reverted in left edge of epoch range
			epochRange:              citypes.EpochRange{100, 501},
			firstReverted:           100,
			expectedPrunedEpochFrom: 100,
		},
		{ // first reverted in right edge of epoch range
			epochRange:              citypes.EpochRange{100, 501},
			firstReverted:           501,
			expectedPrunedEpochFrom: 501,
		},
		{ // first reverted out right side of epoch range
			epochRange:              citypes.EpochRange{100, 501},
			firstReverted:           600,
			expectedPrunedEpochFrom: math.MaxUint64, // should never prune at all
		},
		{ // first reverted out left side of epoch range
			epochRange:              citypes.EpochRange{100, 501},
			firstReverted:           10,
			expectedPrunedEpochFrom: 100,
		},
	}

	for i, tc := range testCases {
		t.Logf(">>>>>> run testcase %v", i+1)

		// Epoch reverted checker
		checker := func(epochNo uint64) (bool, error) {
			t.Logf("check epoch: %v", epochNo)
			if epochNo >= tc.firstReverted {
				return true, nil
			}
			return false, nil
		}
		searcher := func(epochRange citypes.EpochRange) (uint64, error) {
			return syncer.findFirstRevertedEpochInRange(epochRange, checker)
		}
		pruner := func(epochRange citypes.EpochRange) error {
			assert.Equal(t, tc.expectedPrunedEpochFrom, epochRange.EpochFrom)
			return nil
		}
		err := syncer.ensureEpochRangeNotRerverted(tc.epochRange, searcher, pruner)
		assert.Nil(t, err)
	}
}
