package cmd

import (
	"context"
	"sync"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store"
	cisync "github.com/Conflux-Chain/confura/sync"
	"github.com/Conflux-Chain/confura/sync/catchup"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// sync boot options
	syncOpt struct {
		dbSyncEnabled  bool
		kvSyncEnabled  bool
		ethSyncEnabled bool
		catchupEnabled bool
	}

	// catch up settings
	catchupSetting struct {
		epochFrom, epochTo uint64
		adaptive           bool
		benchmark          bool
	}

	syncCmd = &cobra.Command{
		Use:   "sync",
		Short: "Start sync service, including DB/KV/ETH sync, as well as fast catchup",
		Run:   startSyncService,
	}
)

func init() {
	// boot flag for core space DB sync
	syncCmd.Flags().BoolVar(
		&syncOpt.dbSyncEnabled, "db", false, "start core space DB sync server",
	)

	// boot flag for core space DB sync
	syncCmd.Flags().BoolVar(
		&syncOpt.kvSyncEnabled, "kv", false, "start core space KV sync server",
	)

	// boot flag for evm space sync
	syncCmd.Flags().BoolVar(
		&syncOpt.ethSyncEnabled, "eth", false, "start ETH sync server",
	)

	// boot flag for core space fast catch-up
	syncCmd.Flags().BoolVar(
		&syncOpt.catchupEnabled, "catchup", false, "start core space fast catchup server",
	)

	// load fast catchup settings from command line arguments
	syncCmd.Flags().Uint64Var(
		&catchupSetting.epochFrom, "start", 0,
		"the epoch from which fast catch-up sync will start",
	)
	syncCmd.Flags().Uint64Var(
		&catchupSetting.epochTo, "end", 0,
		"the epoch until which fast catch-up sync will end",
	)
	syncCmd.Flags().BoolVar(
		&catchupSetting.adaptive, "adaptive", false,
		"automatically adjust target epoch number to the latest stable epoch",
	)
	syncCmd.Flags().BoolVar(
		&catchupSetting.benchmark, "benchmark", true,
		"benchmarking the performance during fast catch-up sync",
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

	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	syncCtx := util.MustInitSyncContext(storeCtx)
	defer syncCtx.Close()

	var subs []cisync.EpochSubscriber

	if syncOpt.dbSyncEnabled { // start DB sync
		syncer := startSyncCfxDatabase(ctx, &wg, syncCtx)
		subs = append(subs, syncer)
	}

	if syncOpt.kvSyncEnabled { // start KV sync
		if syncer := startSyncCfxCache(ctx, &wg, syncCtx); syncer != nil {
			subs = append(subs, syncer)
		}
	}

	if len(subs) > 0 { // monitor pivot chain switch via pub/sub
		logrus.Info("Start to pub/sub epoch to monitor pivot chain switch")
		go cisync.MustSubEpoch(ctx, &wg, syncCtx.SubCfx, subs...)
	}

	if syncOpt.catchupEnabled { // start fast catchup
		startCatchupSyncCfxDatabase(ctx, &wg, syncCtx)
	}

	if syncOpt.ethSyncEnabled { // start ETH sync
		startSyncEthDatabase(ctx, &wg, syncCtx)
	}

	util.GracefulShutdown(&wg, cancel)
}

// startSyncServiceAdaptively adaptively starts kinds of sync server per to store instances.
func startSyncServiceAdaptively(ctx context.Context, wg *sync.WaitGroup, syncCtx util.SyncContext) {
	if syncCtx.CfxDB == nil && syncCtx.CfxCache == nil && syncCtx.EthDB == nil {
		logrus.Fatal("No data sync configured")
	}

	var subs []cisync.EpochSubscriber

	if syncCtx.CfxDB != nil { // start DB sync
		syncer := startSyncCfxDatabase(ctx, wg, syncCtx)
		subs = append(subs, syncer)
	}

	if syncCtx.CfxCache != nil { // start KV sync
		if syncer := startSyncCfxCache(ctx, wg, syncCtx); syncer != nil {
			subs = append(subs, syncer)
		}
	}

	if len(subs) > 0 { // monitor pivot chain switch via pub/sub
		logrus.Info("Start to pub/sub epoch to monitor pivot chain switch")
		go cisync.MustSubEpoch(ctx, wg, syncCtx.SubCfx, subs...)
	}

	if syncCtx.EthDB != nil { // start ETH sync
		startSyncEthDatabase(ctx, wg, syncCtx)
	}
}

func startSyncCfxDatabase(ctx context.Context, wg *sync.WaitGroup, syncCtx util.SyncContext) *cisync.DatabaseSyncer {
	logrus.Info("Start to sync core space blockchain data into database")

	syncer := cisync.MustNewDatabaseSyncer(syncCtx.SyncCfx, syncCtx.CfxDB)
	go syncer.Sync(ctx, wg)

	// start core space db prune
	go syncCtx.CfxDB.Prune()

	return syncer
}

func startSyncCfxCache(ctx context.Context, wg *sync.WaitGroup, syncCtx util.SyncContext) *cisync.KVCacheSyncer {
	if store.StoreConfig().IsChainBlockDisabled() &&
		store.StoreConfig().IsChainTxnDisabled() &&
		store.StoreConfig().IsChainReceiptDisabled() {
		// KV sync only syncs block, transaction and receipt data. If all of them are disabled,
		// nothing needs to sync, just stop right here.
		return nil
	}

	logrus.Info("Start to sync core space blockchain data into cache")

	csyncer := cisync.MustNewKVCacheSyncer(syncCtx.SyncCfx, syncCtx.CfxCache)
	go csyncer.Sync(ctx, wg)

	// start core space cache prune
	cpruner := cisync.MustNewKVCachePruner(syncCtx.CfxCache)
	go cpruner.Prune(ctx, wg)

	return csyncer
}

func startSyncEthDatabase(ctx context.Context, wg *sync.WaitGroup, syncCtx util.SyncContext) {
	logrus.Info("Start to sync evm space blockchain data into database")

	ethSyncer := cisync.MustNewEthSyncer(syncCtx.SyncEth, syncCtx.EthDB)
	go ethSyncer.Sync(ctx, wg)

	// start evm space db prune
	go syncCtx.EthDB.Prune()
}

func startCatchupSyncCfxDatabase(ctx context.Context, wg *sync.WaitGroup, syncCtx util.SyncContext) {
	logrus.Info("Start to fast catch-up sync core space blockchain data into database")

	if !catchupSetting.adaptive && catchupSetting.epochFrom >= catchupSetting.epochTo {
		logrus.Info("Fast catch-up sync skipped due to start epoch >= end epoch for non-adaptive mode")
		return
	}

	syncer := catchup.MustNewSyncer(
		syncCtx.SyncCfx,
		syncCtx.CfxDB,
		catchup.WithBenchmark(catchupSetting.benchmark),
		catchup.WithAdaptive(catchupSetting.adaptive),
		catchup.WithEpochFrom(catchupSetting.epochFrom),
		catchup.WithEpochTo(catchupSetting.epochTo),
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		syncer.Sync(ctx)
	}()
}
