package cmd

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/conflux-chain/conflux-infura/cmd/test"
	"github.com/conflux-chain/conflux-infura/config"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	flagVersion       bool // print version and exit
	nodeServerEnabled bool // node management service
	rpcServerEnabled  bool // Conflux public RPC service
	syncServerEnabled bool // data sync/prune service

	rootCmd = &cobra.Command{
		Use:   "conflux-infura",
		Short: "Conflux infura provides scalable RPC service",
		Run:   start,
	}
)

func init() {
	rootCmd.Flags().BoolVarP(&flagVersion, "version", "v", false, "If true, print version and exit")

	rootCmd.Flags().BoolVar(&nodeServerEnabled, "nm", false, "whether to start node management service")
	rootCmd.Flags().BoolVar(&rpcServerEnabled, "rpc", false, "whether to start Conflux public RPC service")
	rootCmd.Flags().BoolVar(&syncServerEnabled, "sync", false, "whether to start data sync/prune service")

	rootCmd.AddCommand(test.Cmd)
}

func start(cmd *cobra.Command, args []string) {
	if flagVersion {
		config.DumpVersionInfo()
		return
	}

	if !nodeServerEnabled && !rpcServerEnabled && !syncServerEnabled {
		logrus.Fatal("No services started")
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	storeCtx := mustInitStoreContext(syncServerEnabled)
	defer storeCtx.Close()

	if syncServerEnabled {
		syncCtx := mustInitSyncContext(storeCtx)
		defer syncCtx.Close()
		startSyncService(ctx, wg, syncCtx)
	}

	if rpcServerEnabled {
		startNativeSpaceRpcServer(ctx, wg, storeCtx)
		startEvmSpaceRpcServer(ctx, wg, storeCtx)
		startNativeSpaceBridgeRpcServer(ctx, wg)
	}

	if nodeServerEnabled {
		startNodeServer(ctx, wg)
	}

	util.GracefulShutdown(wg, cancel)
}

func startNodeServer(ctx context.Context, wg *sync.WaitGroup) {
	server := node.NewServer()
	endpoint := node.Config().Endpoint
	go server.MustServeGraceful(ctx, wg, endpoint, util.RpcProtocolHttp)
}

// Execute is the command line entrypoint.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
