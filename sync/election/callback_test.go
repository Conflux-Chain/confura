package election

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeadershipCallbacks(t *testing.T) {
	lm := NewDlockLeaderManager(nil, Config{}, "test")
	var events []LeaderStatus
	lm.OnElected(func(context.Context, LeaderManager) {
		require.Equal(t, StatusElected, lm.electionStatus.Load())
		events = append(events, StatusElected)
	})
	lm.OnOusted(func(context.Context, LeaderManager) {
		require.Equal(t, StatusOusted, lm.electionStatus.Load())
		events = append(events, StatusOusted)
	})
	for range 2 {
		lm.onElected(context.Background())
		lm.onOusted(context.Background())
	}
	require.Equal(t, []LeaderStatus{StatusElected, StatusOusted, StatusElected, StatusOusted}, events)
}
