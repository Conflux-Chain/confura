package cmd

import (
	"context"
	"fmt"
	"sync"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/node"
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
		RunE:  startNodeManagerService,
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

func startNodeManagerService(cmd *cobra.Command, args []string) error {
	if !nmOpt.cfxEnabled && !nmOpt.ethEnabled {
		return fmt.Errorf("No node manager server specified")
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

	if nmOpt.cfxEnabled {
		if err := startNativeSpaceNodeServer(ctx, wg, storeCtx); err != nil {
			return err
		}
	}

	if nmOpt.ethEnabled {
		if err := startEvmSpaceNodeServer(ctx, wg, storeCtx); err != nil {
			return err
		}
	}

	life.Wait()
	return life.Err()
}

func startNativeSpaceNodeServer(ctx context.Context, wg *sync.WaitGroup, storeCtx util.StoreContext) error {
	return node.Factory().StartServer(ctx, wg, storeCtx.GetCfxCommonStore())
}

func startEvmSpaceNodeServer(ctx context.Context, wg *sync.WaitGroup, storeCtx util.StoreContext) error {
	return node.EthFactory().StartServer(ctx, wg, storeCtx.GetEthCommonStore())
}
