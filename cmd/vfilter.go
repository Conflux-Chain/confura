package cmd

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/Conflux-Chain/confura/cmd/util"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/Conflux-Chain/confura/virtualfilter"
)

var (
	// virtual filter boot options
	vfOpt struct {
		cfxEnabled bool
		ethEnabled bool
	}

	virtualFilterCmd = &cobra.Command{
		Use:   "vf",
		Short: "Start virtual filter service",
		Run:   startVirtualFilterService,
	}
)

func init() {
	// boot flag for core space
	virtualFilterCmd.Flags().BoolVar(
		&vfOpt.cfxEnabled, "cfx", false, "start core space virtual filter server",
	)

	// boot flag for evm space
	virtualFilterCmd.Flags().BoolVar(
		&vfOpt.ethEnabled, "eth", false, "start evm space virtual filter server",
	)

	rootCmd.AddCommand(virtualFilterCmd)
}

func startVirtualFilterService(*cobra.Command, []string) {
	if !vfOpt.cfxEnabled && !vfOpt.ethEnabled {
		logrus.Fatal("No virtual filter server specified")
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	storeCtx := util.MustInitStoreContext()
	defer storeCtx.Close()

	if vfOpt.cfxEnabled {
		startCoreSpaceVirtualFilterServer(ctx, &wg, storeCtx)
	}

	if vfOpt.ethEnabled {
		startEvmSpaceVirtualFilterServer(ctx, &wg, storeCtx)
	}

	util.GracefulShutdown(&wg, cancel)
}

// startEvmSpaceVirtualFilterServer starts evm space virtual filter RPC server
func startEvmSpaceVirtualFilterServer(ctx context.Context, wg *sync.WaitGroup, storeCtx util.StoreContext) {
	// serve HTTP endpoint
	vfServer, httpEndpoint := virtualfilter.MustNewEvmSpaceServerFromViper(
		util.GracefulShutdownContext{Ctx: ctx, Wg: wg},
		storeCtx.EthDB.VirtualFilterLogStore,
	)

	go vfServer.MustServeGraceful(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp)
}

// startCoreSpaceVirtualFilterServer starts core space virtual filter RPC server
func startCoreSpaceVirtualFilterServer(ctx context.Context, wg *sync.WaitGroup, storeCtx util.StoreContext) {
	// serve HTTP endpoint
	vfServer, httpEndpoint := virtualfilter.MustNewCoreSpaceServerFromViper(
		util.GracefulShutdownContext{Ctx: ctx, Wg: wg},
		storeCtx.CfxDB.VirtualFilterLogStore,
	)

	go vfServer.MustServeGraceful(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp)
}
