package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util/metrics"
)

func withLogQueryStats(ctx context.Context, rpcMethod string) (context.Context, func()) {
	ctx, stats := store.NewContextWithLogQueryStats(ctx)

	return ctx, func() {
		snapshot := stats.Snapshot()
		metrics.Registry.RPC.LogQueryDBRowScans(rpcMethod).Update(snapshot.DBRowScans)
		metrics.Registry.RPC.LogQueryPartitionHits(rpcMethod).Update(snapshot.PartitionHits)
		metrics.Registry.RPC.LogQueryFanOuts(rpcMethod).Update(snapshot.FanOuts)
		metrics.Registry.RPC.LogQueryReorgRetries(rpcMethod).Update(snapshot.ReorgRetries)
	}
}
