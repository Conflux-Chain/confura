package cmd

import (
	"context"
	"fmt"
	"sync"

	"github.com/Conflux-Chain/confura/cmd/util"
	cisync "github.com/Conflux-Chain/confura/sync"
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
		RunE:  startSyncService,
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

func startSyncService(cmd *cobra.Command, args []string) error {
	if !syncOpt.dbSyncEnabled && !syncOpt.ethSyncEnabled && !syncOpt.traceSyncEnabled {
		return fmt.Errorf("No Sync server specified")
	}
	life, err := util.NewLifecycle(cmd.Context())
	if err != nil {
		return err
	}
	defer life.Close()
	ctx, wg := life.Ctx, &life.Wg
	storeCtx, err := util.InitStoreContext("cfx", "eth")
	if err != nil {
		return err
	}
	var syncCtx util.SyncContext
	defer func() { life.Shutdown(); syncCtx.Close(); storeCtx.Close() }()
	syncCtx, err = util.InitSyncContext(storeCtx)
	if err != nil {
		return err
	}

	if syncOpt.dbSyncEnabled { // start DB sync
		if err := startSyncCfxDatabase(ctx, wg, syncCtx, life.Fail); err != nil {
			return err
		}
	}

	if syncOpt.ethSyncEnabled { // start ETH sync
		if err := startSyncEthDatabase(ctx, wg, syncCtx, life.Fail); err != nil {
			return err
		}
	}

	if syncOpt.traceSyncEnabled { // start trace log sync
		if err := startSyncTraceLog(ctx, wg, syncCtx, life.Fail); err != nil {
			return err
		}
	}

	life.Wait()
	return life.Err()
}

// startSyncServiceAdaptively starts only spaces with configured stores.
func startSyncServiceAdaptively(ctx context.Context, wg *sync.WaitGroup, sc util.SyncContext, fail func(error)) error {
	if sc.CfxDB == nil && sc.EthDB == nil {
		return fmt.Errorf("no data sync configured")
	}
	if sc.CfxDB != nil {
		if err := startSyncCfxDatabase(ctx, wg, sc, fail); err != nil {
			return err
		}
	}
	if sc.EthDB != nil {
		if err := startSyncEthDatabase(ctx, wg, sc, fail); err != nil {
			return err
		}
	}
	return nil
}

func startSyncCfxDatabase(ctx context.Context, wg *sync.WaitGroup, sc util.SyncContext, fail func(error)) error {
	if sc.CfxDB == nil {
		return fmt.Errorf("cfx database is required for sync")
	}
	s, err := cisync.NewDatabaseSyncer(sc.SyncCfxs, sc.CfxDB)
	if err != nil {
		return err
	}
	util.RunTracked(ctx, wg, func(ctx context.Context, inner *sync.WaitGroup) { fail(s.Sync(ctx, inner)) })
	sc.CfxDB.Prune()
	return nil
}

func startSyncEthDatabase(ctx context.Context, wg *sync.WaitGroup, sc util.SyncContext, fail func(error)) error {
	if sc.EthDB == nil {
		return fmt.Errorf("eth database is required for sync")
	}
	s, err := cisync.NewEthSyncer(sc.SyncEths, sc.EthDB)
	if err != nil {
		return err
	}
	util.RunTracked(ctx, wg, func(ctx context.Context, inner *sync.WaitGroup) { fail(s.Sync(ctx, inner)) })
	sc.EthDB.Prune()
	return nil
}

func startSyncTraceLog(ctx context.Context, wg *sync.WaitGroup, sc util.SyncContext, fail func(error)) error {
	if sc.CfxDB == nil {
		return fmt.Errorf("cfx database is required for trace sync")
	}
	s, err := cisync.NewTraceLogSyncer(sc.SyncCfxs, sc.CfxDB)
	if err != nil {
		return err
	}
	util.RunTracked(ctx, wg, func(ctx context.Context, inner *sync.WaitGroup) { fail(s.Sync(ctx, inner)) })
	return nil
}
