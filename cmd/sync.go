package cmd

import (
	"context"
	"sync"

	cisync "github.com/conflux-chain/conflux-infura/sync"
	"github.com/sirupsen/logrus"
)

func startSyncService(ctx context.Context, wg *sync.WaitGroup, syncCtx syncContext) {
	if syncCtx.cfxDB == nil && syncCtx.cfxCache == nil && syncCtx.ethDB == nil {
		logrus.Fatal("No data sync configured")
	}

	var subs []cisync.EpochSubscriber

	if syncCtx.cfxDB != nil {
		syncer := startSyncCfxDatabase(ctx, wg, syncCtx)
		subs = append(subs, syncer)
	}

	if syncCtx.cfxCache != nil {
		syncer := startSyncCfxCache(ctx, wg, syncCtx)
		subs = append(subs, syncer)
	}

	if len(subs) > 0 {
		// Monitor pivot chain switch via pub/sub
		logrus.Info("Start to pub/sub epoch to monitor pivot chain switch")
		go cisync.MustSubEpoch(ctx, wg, syncCtx.subCfx, subs...)
	}

	if syncCtx.ethDB != nil {
		startSyncEthDatabase(ctx, wg, syncCtx)
	}
}

func startSyncCfxDatabase(ctx context.Context, wg *sync.WaitGroup, syncCtx syncContext) *cisync.DatabaseSyncer {
	// Sync data to db
	logrus.Info("Start to sync CFX blockchain data into database")
	syncer := cisync.MustNewDatabaseSyncer(syncCtx.syncCfx, syncCtx.cfxDB)
	go syncer.Sync(ctx, wg)

	// Prune data from db
	logrus.Info("Start to prune CFX blockchain data from database")
	pruner := cisync.MustNewDBPruner(syncCtx.cfxDB)
	go pruner.Prune(ctx, wg)

	return syncer
}

func startSyncCfxCache(ctx context.Context, wg *sync.WaitGroup, syncCtx syncContext) *cisync.KVCacheSyncer {
	// Sync data to cache
	logrus.Info("Start to sync CFX blockchain data into cache")
	csyncer := cisync.MustNewKVCacheSyncer(syncCtx.syncCfx, syncCtx.cfxCache)
	go csyncer.Sync(ctx, wg)

	// Prune data from cache
	logrus.Info("Start to prune CFX blockchain data from cache")
	cpruner := cisync.MustNewKVCachePruner(syncCtx.cfxCache)
	go cpruner.Prune(ctx, wg)

	return csyncer
}

func startSyncEthDatabase(ctx context.Context, wg *sync.WaitGroup, syncCtx syncContext) {
	// Sync data to database
	logrus.Info("Start to sync ETH blockchain data into database")
	ethSyncer := cisync.MustNewEthSyncer(syncCtx.syncEth, syncCtx.ethDB)
	go ethSyncer.Sync(ctx, wg)

	// Prune data from database
	logrus.Info("Start to prune ETH blockchain data from database")
	ethdbPruner := cisync.MustNewDBPruner(syncCtx.ethDB)
	go ethdbPruner.Prune(ctx, wg)
}
