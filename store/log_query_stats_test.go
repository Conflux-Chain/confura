package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogQueryStatsContext(t *testing.T) {
	ctx, stats := NewContextWithLogQueryStats(context.Background())

	AddLogQueryDBRowScans(ctx, 10)
	AddLogQueryPartitionHits(ctx, 2)
	AddLogQueryFanOuts(ctx, 3)
	AddLogQueryReorgRetries(ctx, 1)

	require.Same(t, stats, LogQueryStatsFromContext(ctx))
	require.Equal(t, LogQueryStatsSnapshot{
		DBRowScans:    10,
		PartitionHits: 2,
		FanOuts:       3,
		ReorgRetries:  1,
	}, stats.Snapshot())
}

func TestLogQueryStatsNoopWithoutContext(t *testing.T) {
	ctx := context.Background()

	AddLogQueryDBRowScans(ctx, 10)
	AddLogQueryPartitionHits(ctx, 2)
	AddLogQueryFanOuts(ctx, 3)
	AddLogQueryReorgRetries(ctx, 1)

	require.Nil(t, LogQueryStatsFromContext(ctx))
}
