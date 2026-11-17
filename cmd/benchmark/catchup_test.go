package benchmark

import (
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestCatchUpRange(t *testing.T) {
	for _, tc := range []struct {
		start, count, end uint64
		bad               bool
	}{
		{0, 0, 0, false}, {1, 1, 1, false}, {math.MaxUint64, 1, math.MaxUint64, false},
		{math.MaxUint64, 2, 0, true}, {2, math.MaxUint64, 0, true},
	} {
		end, err := catchUpRangeEnd(tc.start, tc.count)
		require.Equal(t, tc.bad, err != nil)
		if !tc.bad {
			require.Equal(t, tc.end, end)
		}
	}
}
