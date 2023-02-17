package cmd

import (
	"context"
	"sync"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/util/rpc"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// node management boot options
	nmOpt struct {
		cfxEnabled bool
		ethEnabled bool
	}

	nmCmd = &cobra.Command{
		Use:   "nm",
		Short: "Start node management service, including core space and evm space node managers",
		Run:   startNodeManagerService,
	}
)

func init() {
	// boot flag for core space
	nmCmd.Flags().BoolVar(
		&nmOpt.cfxEnabled, "cfx", false, "start core space node manager server",
	)

	// boot flag for evm space
	nmCmd.Flags().BoolVar(
		&nmOpt.ethEnabled, "eth", false, "start evm space node manager server",
	)

	rootCmd.AddCommand(nmCmd)
}

func startNodeManagerService(*cobra.Command, []string) {
	if !nmOpt.cfxEnabled && !nmOpt.ethEnabled {
		logrus.Fatal("No node mananger server specified")
	}

	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	if nmOpt.cfxEnabled {
		startNativeSpaceNodeServer(ctx, &wg, storeCtx)
	}

	if nmOpt.ethEnabled {
		startEvmSpaceNodeServer(ctx, &wg, storeCtx)
	}

	util.GracefulShutdown(&wg, cancel)
}

func startNativeSpaceNodeServer(ctx context.Context, wg *sync.WaitGroup, storeCtx util.StoreContext) {
	server, endpoint := node.Factory().CreatRpcServer(storeCtx.CfxDB)
	go server.MustServeGraceful(ctx, wg, endpoint, rpc.ProtocolHttp)
}

func startEvmSpaceNodeServer(ctx context.Context, wg *sync.WaitGroup, storeCtx util.StoreContext) {
	server, endpoint := node.EthFactory().CreatRpcServer(storeCtx.EthDB)
	go server.MustServeGraceful(ctx, wg, endpoint, rpc.ProtocolHttp)
}
