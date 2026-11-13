package sync

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/poll"
	"github.com/stretchr/testify/require"
)

func TestTraceReorgWindowResumesCommittedProgress(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		next, finalized, anchor uint64
	}{
		{"genesis", 0, 10, 0},
		{"first epoch", 1, 0, 0},
		{"unfinalized history", 14, 10, 10},
		{"finality advanced after catchup", 10, 20, 9},
		{"next equals finalized", 10, 10, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			params, err := loadTraceReorgWindow(tc.next, tc.finalized, func(from, to uint64) (map[uint64]string, error) {
				called = true
				require.Equal(t, tc.anchor, from)
				require.Equal(t, tc.next-1, to)
				hashes := make(map[uint64]string)
				for n := from; n <= to; n++ {
					hashes[n] = fmt.Sprint(n)
				}
				return hashes, nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.next != 0, called)
			require.NotContains(t, params.LatestBlocks, tc.anchor)
			window, err := poll.NewReorgWindow(params)
			require.NoError(t, err)
			require.Equal(t, tc.next, window.NextBlockNumber())
			require.Equal(t, tc.anchor, window.FinalizedBlockNumber())
		})
	}
}

func TestTraceReorgWindowRejectsMissingHistory(t *testing.T) {
	for _, hashes := range []map[uint64]string{
		{11: "b", 12: "c"}, {10: "a", 12: "c"}, {10: "a", 11: "b", 12: ""},
	} {
		_, err := loadTraceReorgWindow(13, 10, func(uint64, uint64) (map[uint64]string, error) { return hashes, nil })
		require.ErrorContains(t, err, "missing pivot hash")
	}
	failure := errors.New("database unavailable")
	_, err := loadTraceReorgWindow(13, 10, func(uint64, uint64) (map[uint64]string, error) { return nil, failure })
	require.ErrorIs(t, err, failure)
}
