package cmd

import (
	"context"
	"sync"

	"github.com/Conflux-Chain/confura/cmd/util"
	cisync "github.com/Conflux-Chain/confura/sync"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// sync boot options
	syncOpt struct {
		dbSyncEnabled    bool
		ethSyncEnabled   bool
		traceSyncEnabled bool
	}

	syncCmd = &cobra.Command{
		Use:   "sync",
		Short: "Start sync service, including DB/ETH sync, as well as fast catchup",
		Run:   startSyncService,
	}
)

func init() {
	// boot flag for core space DB sync
	syncCmd.Flags().BoolVar(
		&syncOpt.dbSyncEnabled, "db", false, "start core space DB sync server",
	)

	// boot flag for evm space sync
	syncCmd.Flags().BoolVar(
		&syncOpt.ethSyncEnabled, "eth", false, "start ETH sync server",
	)

	// boot flag for trace log sync
	syncCmd.Flags().BoolVar(
		&syncOpt.traceSyncEnabled, "trace", false, "start trace log sync server",
	)

	rootCmd.AddCommand(syncCmd)
}

func startSyncService(*cobra.Command, []string) {
	if !syncOpt.dbSyncEnabled && !syncOpt.ethSyncEnabled && !syncOpt.traceSyncEnabled {
		logrus.Fatal("No Sync server specified")
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	syncCtx := util.MustInitSyncContext(storeCtx)
	defer syncCtx.Close()

	if syncOpt.dbSyncEnabled { // start DB sync
		startSyncCfxDatabase(ctx, &wg, syncCtx)
	}

	if syncOpt.ethSyncEnabled { // start ETH sync
		startSyncEthDatabase(ctx, &wg, syncCtx)
	}

	if syncOpt.traceSyncEnabled { // start trace log sync
		startSyncTraceLog(ctx, &wg, syncCtx)
	}

	util.GracefulShutdown(&wg, cancel)
}

// startSyncServiceAdaptively adaptively starts kinds of sync server per to store instances.
func startSyncServiceAdaptively(ctx context.Context, wg *sync.WaitGroup, syncCtx util.SyncContext) {
	if syncCtx.CfxDB == nil && syncCtx.EthDB == nil {
		logrus.Fatal("No data sync configured")
	}

	if syncCtx.CfxDB != nil { // start DB sync
		startSyncCfxDatabase(ctx, wg, syncCtx)
	}

	if syncCtx.EthDB != nil { // start ETH sync
		startSyncEthDatabase(ctx, wg, syncCtx)
	}
}

func startSyncCfxDatabase(ctx context.Context, wg *sync.WaitGroup, syncCtx util.SyncContext) *cisync.DatabaseSyncer {
	logrus.Info("Start to sync core space blockchain data into database")

	syncer := cisync.MustNewDatabaseSyncer(syncCtx.SyncCfxs, syncCtx.CfxDB)
	go syncer.Sync(ctx, wg)

	// start core space db prune
	go syncCtx.CfxDB.Prune()

	return syncer
}

func startSyncEthDatabase(ctx context.Context, wg *sync.WaitGroup, syncCtx util.SyncContext) {
	logrus.Info("Start to sync evm space blockchain data into database")

	ethSyncer := cisync.MustNewEthSyncer(syncCtx.SyncEths, syncCtx.EthDB)
	go ethSyncer.Sync(ctx, wg)

	// start evm space db prune
	go syncCtx.EthDB.Prune()
}

func startSyncTraceLog(ctx context.Context, wg *sync.WaitGroup, syncCtx util.SyncContext) {
	logrus.Info("Start to sync trace log into database")

	traceLogSyncer := cisync.MustNewTraceLogSyncer(syncCtx.SyncCfxs, syncCtx.CfxDB)
	go traceLogSyncer.MustSync(ctx, wg)
}
