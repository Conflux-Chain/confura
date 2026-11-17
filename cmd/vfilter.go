package cmd

import (
	"context"
	"fmt"
	"sync"

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
		RunE:  startVirtualFilterService,
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

func startVirtualFilterService(cmd *cobra.Command, args []string) error {
	if !vfOpt.cfxEnabled && !vfOpt.ethEnabled {
		return fmt.Errorf("No virtual filter server specified")
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
	defer func() { life.Shutdown(); storeCtx.Close() }()

	if vfOpt.cfxEnabled {
		if err := startCoreSpaceVirtualFilterServer(ctx, wg, storeCtx); err != nil {
			return err
		}
	}

	if vfOpt.ethEnabled {
		if err := startEvmSpaceVirtualFilterServer(ctx, wg, storeCtx); err != nil {
			return err
		}
	}

	life.Wait()
	return life.Err()
}

// startEvmSpaceVirtualFilterServer starts evm space virtual filter RPC server
func startEvmSpaceVirtualFilterServer(ctx context.Context, wg *sync.WaitGroup, storeCtx util.StoreContext) error {
	if storeCtx.EthDB == nil {
		return fmt.Errorf("eth database is required for virtual filters")
	}
	// serve HTTP endpoint
	vfServer, httpEndpoint, err := virtualfilter.NewEvmSpaceServerFromViper(
		util.GracefulShutdownContext{Ctx: ctx, Wg: wg},
		storeCtx.EthDB.VirtualFilterLogStore,
	)

	if err != nil {
		return err
	}
	return vfServer.Start(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp)
}

// startCoreSpaceVirtualFilterServer starts core space virtual filter RPC server
func startCoreSpaceVirtualFilterServer(ctx context.Context, wg *sync.WaitGroup, storeCtx util.StoreContext) error {
	if storeCtx.CfxDB == nil {
		return fmt.Errorf("cfx database is required for virtual filters")
	}
	// serve HTTP endpoint
	vfServer, httpEndpoint, err := virtualfilter.NewCoreSpaceServerFromViper(
		util.GracefulShutdownContext{Ctx: ctx, Wg: wg},
		storeCtx.CfxDB.VirtualFilterLogStore,
	)

	if err != nil {
		return err
	}
	return vfServer.Start(ctx, wg, httpEndpoint, rpcutil.ProtocolHttp)
}
