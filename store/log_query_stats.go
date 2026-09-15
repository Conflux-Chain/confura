package store

import (
	"context"
	"sync/atomic"
)

const logQueryStatsKey contextKey = "Log-Query-Stats"

type LogQueryStats struct {
	dbRowScans    atomic.Int64
	partitionHits atomic.Int64
	fanOuts       atomic.Int64
	reorgRetries  atomic.Int64
}

type LogQueryStatsSnapshot struct {
	DBRowScans    int64
	PartitionHits int64
	FanOuts       int64
	ReorgRetries  int64
}

func NewContextWithLogQueryStats(ctx context.Context) (context.Context, *LogQueryStats) {
	if stats := LogQueryStatsFromContext(ctx); stats != nil {
		return ctx, stats
	}

	stats := new(LogQueryStats)
	return context.WithValue(ctx, logQueryStatsKey, stats), stats
}

func LogQueryStatsFromContext(ctx context.Context) *LogQueryStats {
	stats, _ := ctx.Value(logQueryStatsKey).(*LogQueryStats)
	return stats
}

func AddLogQueryDBRowScans(ctx context.Context, n int64) {
	if n <= 0 {
		return
	}
	if stats := LogQueryStatsFromContext(ctx); stats != nil {
		stats.dbRowScans.Add(n)
	}
}

func AddLogQueryPartitionHits(ctx context.Context, n int64) {
	if n <= 0 {
		return
	}
	if stats := LogQueryStatsFromContext(ctx); stats != nil {
		stats.partitionHits.Add(n)
	}
}

func AddLogQueryFanOuts(ctx context.Context, n int64) {
	if n <= 0 {
		return
	}
	if stats := LogQueryStatsFromContext(ctx); stats != nil {
		stats.fanOuts.Add(n)
	}
}

func AddLogQueryReorgRetries(ctx context.Context, n int64) {
	if n <= 0 {
		return
	}
	if stats := LogQueryStatsFromContext(ctx); stats != nil {
		stats.reorgRetries.Add(n)
	}
}

func (s *LogQueryStats) Snapshot() LogQueryStatsSnapshot {
	if s == nil {
		return LogQueryStatsSnapshot{}
	}

	return LogQueryStatsSnapshot{
		DBRowScans:    s.dbRowScans.Load(),
		PartitionHits: s.partitionHits.Load(),
		FanOuts:       s.fanOuts.Load(),
		ReorgRetries:  s.reorgRetries.Load(),
	}
}
