package sync

import (
	"math"
	"testing"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/stretchr/testify/assert"
)

func TestFindFirstRevertedEpochInRange(t *testing.T) {
	syncer := &DatabaseSyncer{}

	testCases := []struct {
		epochRange    citypes.RangeUint64
		firstReverted uint64
		expected      uint64
	}{
		{ // first reverted in middle of epoch range
			epochRange:    citypes.RangeUint64{From: 10, To: 50},
			firstReverted: 20,
			expected:      20,
		},
		{ // first reverted in right edge of epoch range
			epochRange:    citypes.RangeUint64{From: 10, To: 50},
			firstReverted: 50,
			expected:      50,
		},
		{ // first reverted in left edge of epoch range
			epochRange:    citypes.RangeUint64{From: 10, To: 50},
			firstReverted: 1,
			expected:      10,
		},
		{ // first reverted out of right side of epoch range
			epochRange:    citypes.RangeUint64{From: 10, To: 50},
			firstReverted: 51,
			expected:      0,
		},
		{ // first reverted out of left side of epoch range
			epochRange:    citypes.RangeUint64{From: 10, To: 50},
			firstReverted: 1,
			expected:      10,
		},
	}

	for i, tc := range testCases {
		t.Logf(">>>>>> run testcase %v", i+1)

		// Epoch reverted checker
		checker := func(cfx sdk.ClientOperator, s store.Store, epochNo uint64) (bool, error) {
			t.Logf("check epoch: %v", epochNo)
			if epochNo >= tc.firstReverted {
				return true, nil
			}
			return false, nil
		}
		res, err := findFirstRevertedEpochInRange(syncer.cfx, syncer.db, tc.epochRange, checker)
		assert.Nil(t, err)
		assert.Equal(t, tc.expected, res)
	}
}

func TestEnsureEpochRangeNotRerverted(t *testing.T) {
	syncer := &DatabaseSyncer{}

	testCases := []struct {
		epochRange              citypes.RangeUint64
		firstReverted           uint64
		expectedPrunedEpochFrom uint64
	}{
		{ // first reverted in middle of epoch range
			epochRange:              citypes.RangeUint64{From: 100, To: 501},
			firstReverted:           200,
			expectedPrunedEpochFrom: 200,
		},
		{ // first reverted in left edge of epoch range
			epochRange:              citypes.RangeUint64{From: 100, To: 501},
			firstReverted:           100,
			expectedPrunedEpochFrom: 100,
		},
		{ // first reverted in right edge of epoch range
			epochRange:              citypes.RangeUint64{From: 100, To: 501},
			firstReverted:           501,
			expectedPrunedEpochFrom: 501,
		},
		{ // first reverted out right side of epoch range
			epochRange:              citypes.RangeUint64{From: 100, To: 501},
			firstReverted:           600,
			expectedPrunedEpochFrom: math.MaxUint64, // should never prune at all
		},
		{ // first reverted out left side of epoch range
			epochRange:              citypes.RangeUint64{From: 100, To: 501},
			firstReverted:           10,
			expectedPrunedEpochFrom: 100,
		},
	}

	for i, tc := range testCases {
		t.Logf(">>>>>> run testcase %v", i+1)

		// Epoch reverted checker
		checker := func(cfx sdk.ClientOperator, s store.Store, epochNo uint64) (bool, error) {
			t.Logf("check epoch: %v", epochNo)
			if epochNo >= tc.firstReverted {
				return true, nil
			}
			return false, nil
		}
		searcher := func(cfx sdk.ClientOperator, s store.Store, epochRange citypes.RangeUint64) (uint64, error) {
			return findFirstRevertedEpochInRange(syncer.cfx, syncer.db, epochRange, checker)
		}
		pruner := func(s store.Store, epochRange citypes.RangeUint64) error {
			assert.Equal(t, tc.expectedPrunedEpochFrom, epochRange.From)
			return nil
		}
		err := ensureEpochRangeNotRerverted(syncer.cfx, syncer.db, tc.epochRange, searcher, pruner)
		assert.Nil(t, err)
	}
}
