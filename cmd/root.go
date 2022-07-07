package cmd

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/conflux-chain/conflux-infura/cmd/test"
	"github.com/conflux-chain/conflux-infura/cmd/util"
	"github.com/conflux-chain/conflux-infura/config"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	flagVersion       bool
	nodeServerEnabled bool
	rpcServerEnabled  bool
	syncServerEnabled bool

	rootCmd = &cobra.Command{
		Use:   "Confura",
		Short: "Ethereum Infura like Public RPC Service on Conflux Network.",
		Run:   start,
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

	rootCmd.AddCommand(test.Cmd)
}

func start(cmd *cobra.Command, args []string) {
	// dump version
	if flagVersion {
		config.DumpVersionInfo()
		return
	}

	if !nodeServerEnabled && !rpcServerEnabled && !syncServerEnabled {
		logrus.Fatal("No services started")
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	storeCtx := mustInitStoreContext()
	defer storeCtx.Close()

	if syncServerEnabled { // start sync
		syncCtx := mustInitSyncContext(storeCtx)
		defer syncCtx.Close()

		startSyncServiceAdaptively(ctx, wg, syncCtx)
	}

	if rpcServerEnabled { // start RPC
		startNativeSpaceRpcServer(ctx, wg, storeCtx)
		startEvmSpaceRpcServer(ctx, wg, storeCtx)
		startNativeSpaceBridgeRpcServer(ctx, wg)
	}

	if nodeServerEnabled { // start node management
		startNativeSpaceNodeServer(ctx, wg)
		startEvmSpaceNodeServer(ctx, wg)
	}

	util.GracefulShutdown(wg, cancel)
}

// Execute is the command line entrypoint.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
