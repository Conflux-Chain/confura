package cmd

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/Conflux-Chain/confura/cmd/acl"
	"github.com/Conflux-Chain/confura/cmd/benchmark"
	"github.com/Conflux-Chain/confura/cmd/noderoute"
	"github.com/Conflux-Chain/confura/cmd/ratelimit"
	"github.com/Conflux-Chain/confura/cmd/test"
	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/config"
	"github.com/spf13/cobra"
)

var (
	flagVersion          bool
	nodeServerEnabled    bool
	rpcServerEnabled     bool
	syncServerEnabled    bool
	vfilterServerEnabled bool

	rootCmd = &cobra.Command{
		Use:   "confura",
		Short: "Ethereum Infura like Public RPC Service on Conflux Network.",
		RunE:  start,
	}
)

func init() {
	// print version and exit
	rootCmd.Flags().BoolVarP(
		&flagVersion, "version", "v", false, "If true, print version and exit",
	)

	// boot flag for node management service
	rootCmd.Flags().BoolVar(
		&nodeServerEnabled, "nm", false, "whether to start node management service",
	)

	// boot flag for public RPC service
	rootCmd.Flags().BoolVar(
		&rpcServerEnabled, "rpc", false, "whether to start Confura public RPC service",
	)

	// boot flag for sync service (accompanied with prune)
	rootCmd.Flags().BoolVar(
		&syncServerEnabled, "sync", false, "whether to start data sync/prune service",
	)

	// boot flag for virtual filter service
	rootCmd.Flags().BoolVar(
		&vfilterServerEnabled, "vf", false, "whether to start virtual filter service",
	)

	rootCmd.AddCommand(test.Cmd)
	rootCmd.AddCommand(ratelimit.Cmd)
	rootCmd.AddCommand(noderoute.Cmd)
	rootCmd.AddCommand(acl.Cmd)
	rootCmd.AddCommand(benchmark.Cmd)
}

func start(cmd *cobra.Command, args []string) error {
	// dump version
	if flagVersion {
		config.DumpVersionInfo()
		return nil
	}

	if !nodeServerEnabled && !rpcServerEnabled && !syncServerEnabled && !vfilterServerEnabled {
		return fmt.Errorf("no services started")
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	storeCtx := util.MustInitStoreContext()
	var syncCtx util.SyncContext
	defer func() { cancel(); wg.Wait(); syncCtx.Close(); storeCtx.Close() }()

	if syncServerEnabled { // start sync
		syncCtx = util.MustInitSyncContext(storeCtx)

		startSyncServiceAdaptively(ctx, wg, syncCtx)
	}

	if rpcServerEnabled { // start RPC
		if err := startNativeSpaceRpcServer(ctx, wg, storeCtx); err != nil {
			return err
		}
		if err := startEvmSpaceRpcServer(ctx, wg, storeCtx); err != nil {
			return err
		}
		if err := startNativeSpaceBridgeRpcServer(ctx, wg, storeCtx); err != nil {
			return err
		}
	}

	if nodeServerEnabled { // start node management
		startNativeSpaceNodeServer(ctx, wg, storeCtx)
		startEvmSpaceNodeServer(ctx, wg, storeCtx)
	}

	if vfilterServerEnabled { // start virtual filter
		startEvmSpaceVirtualFilterServer(ctx, wg, storeCtx)
	}

	util.GracefulShutdown(wg, cancel)
	return nil
}

// Execute is the command line entrypoint.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
