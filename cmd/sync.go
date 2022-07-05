package cmd

import (
	"context"
	"sync"

	"github.com/conflux-chain/conflux-infura/cmd/util"
	"github.com/conflux-chain/conflux-infura/store"
	cisync "github.com/conflux-chain/conflux-infura/sync"
	"github.com/conflux-chain/conflux-infura/sync/catchup"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type syncOption struct {
	dbSyncEnabled  bool
	kvSyncEnabled  bool
	ethSyncEnabled bool
	catchupEnabled bool
}

type catchupSetting struct {
	epochFrom, epochTo uint64
	adaptive           bool
	benchmark          bool
}

var (
	syncOpt syncOption
	cuSet   catchupSetting

	syncCmd = &cobra.Command{
		Use:   "sync",
		Short: "Start sync service, including DB/KV/ETH sync and fast catchup servers",
		Run:   startSyncService,
	}
)

func init() {
	syncCmd.Flags().BoolVar(&syncOpt.dbSyncEnabled, "db", false, "Start DB sync server")
	syncCmd.Flags().BoolVar(&syncOpt.kvSyncEnabled, "kv", false, "Start KV sync server")
	syncCmd.Flags().BoolVar(&syncOpt.ethSyncEnabled, "eth", false, "Start ETH sync server")
	syncCmd.Flags().BoolVar(&syncOpt.catchupEnabled, "catchup", false, "Start catchup fast sync server")

	// load catchup fast sync settings from command line arguments
	syncCmd.Flags().Uint64Var(
		&cuSet.epochFrom, "start", 0,
		"the epoch from which catch-up fast sync will start",
	)
	syncCmd.Flags().Uint64Var(
		&cuSet.epochTo, "end", 0,
		"the epoch until which catch-up fast sync will end",
	)
	syncCmd.Flags().BoolVar(
		&cuSet.adaptive, "adaptive", false,
		"automatically adjust target epoch number to the latest stable epoch",
	)
	syncCmd.Flags().BoolVar(
		&cuSet.benchmark, "benchmark", true,
		"benchmarking the fast sync performance during catch-up",
	)

	rootCmd.AddCommand(syncCmd)
}

func startSyncService(*cobra.Command, []string) {
	if !syncOpt.dbSyncEnabled && !syncOpt.kvSyncEnabled &&
		!syncOpt.ethSyncEnabled && !syncOpt.catchupEnabled {
		logrus.Fatal("No Sync server specified")
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	storeCtx := mustInitStoreContext()
	defer storeCtx.Close()

	syncCtx := mustInitSyncContext(storeCtx)
	defer syncCtx.Close()

	var subs []cisync.EpochSubscriber

	if syncOpt.dbSyncEnabled {
		syncer := startSyncCfxDatabase(ctx, &wg, syncCtx)
		subs = append(subs, syncer)
	}

	if syncOpt.kvSyncEnabled {
		if syncer := startSyncCfxCache(ctx, &wg, syncCtx); syncer != nil {
			subs = append(subs, syncer)
		}
	}

	if syncOpt.ethSyncEnabled {
		startSyncEthDatabase(ctx, &wg, syncCtx)
	}

	if syncOpt.catchupEnabled {
		startFastSyncCfxDatabase(ctx, &wg, syncCtx)
	}

	if len(subs) > 0 {
		// Monitor pivot chain switch via pub/sub
		logrus.Info("Start to pub/sub epoch to monitor pivot chain switch")
		go cisync.MustSubEpoch(ctx, &wg, syncCtx.subCfx, subs...)
	}

	util.GracefulShutdown(&wg, cancel)
}

func startSyncServiceAdaptively(ctx context.Context, wg *sync.WaitGroup, syncCtx syncContext) {
	if syncCtx.cfxDB == nil && syncCtx.cfxCache == nil && syncCtx.ethDB == nil {
		logrus.Fatal("No data sync configured")
	}

	var subs []cisync.EpochSubscriber

	if syncCtx.cfxDB != nil {
		syncer := startSyncCfxDatabase(ctx, wg, syncCtx)
		subs = append(subs, syncer)
	}

	if syncCtx.cfxCache != nil {
		if syncer := startSyncCfxCache(ctx, wg, syncCtx); syncer != nil {
			subs = append(subs, syncer)
		}
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
	go syncCtx.cfxDB.Prune()

	return syncer
}

func startSyncCfxCache(ctx context.Context, wg *sync.WaitGroup, syncCtx syncContext) *cisync.KVCacheSyncer {
	if store.StoreConfig().IsChainBlockDisabled() &&
		store.StoreConfig().IsChainTxnDisabled() &&
		store.StoreConfig().IsChainReceiptDisabled() {
		// KV sync only syncs block, transaction and receipt data. If all of them are disabled,
		// nothing to sync, just stop right here.
		return nil
	}

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
	go syncCtx.ethDB.Prune()
}

func startFastSyncCfxDatabase(ctx context.Context, wg *sync.WaitGroup, syncCtx syncContext) {
	// Catch-up fast sync core space epoch data to db
	logrus.Info("Start to catch-up fast sync CFX blockchain data into database")

	if !cuSet.adaptive && cuSet.epochFrom >= cuSet.epochTo {
		logrus.Info("Catch-up fast sync should have end epoch > start epoch for non-adaptive mode")
		return
	}

	syncer := catchup.MustNewSyncer(
		syncCtx.syncCfx, syncCtx.cfxDB,
		catchup.WithBenchmark(cuSet.benchmark),
		catchup.WithAdaptive(cuSet.adaptive),
		catchup.WithEpochFrom(cuSet.epochFrom),
		catchup.WithEpochTo(cuSet.epochTo),
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		syncer.Sync(ctx)
	}()
}
