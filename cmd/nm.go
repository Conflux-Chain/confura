package cmd

import (
	"context"
	"sync"

	"github.com/conflux-chain/conflux-infura/cmd/util"
	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/util/rpc"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type nmOption struct {
	cfxEnabled bool
	ethEnabled bool
}

var (
	nmOpt nmOption

	nmCmd = &cobra.Command{
		Use:   "nm",
		Short: "Start node manager service, including CFX, ETH node managers",
		Run:   startNodeManagerService,
	}
)

func init() {
	nmCmd.Flags().BoolVar(&nmOpt.cfxEnabled, "cfx", false, "Start CFX space node manager server")
	nmCmd.Flags().BoolVar(&nmOpt.ethEnabled, "eth", false, "Start ETH space node manager server")

	rootCmd.AddCommand(nmCmd)
}

func startNodeManagerService(*cobra.Command, []string) {
	if !nmOpt.cfxEnabled && !nmOpt.ethEnabled {
		logrus.Fatal("No node mananger server specified")
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	if nmOpt.cfxEnabled {
		startNativeSpaceNodeServer(ctx, &wg)
	}

	if nmOpt.ethEnabled {
		startEvmSpaceNodeServer(ctx, &wg)
	}

	util.GracefulShutdown(&wg, cancel)
}

func startNativeSpaceNodeServer(ctx context.Context, wg *sync.WaitGroup) {
	server, endpoint := node.Factory().CreatRpcServer()
	go server.MustServeGraceful(ctx, wg, endpoint, rpc.ProtocolHttp)
}

func startEvmSpaceNodeServer(ctx context.Context, wg *sync.WaitGroup) {
	server, endpoint := node.EthFactory().CreatRpcServer()
	go server.MustServeGraceful(ctx, wg, endpoint, rpc.ProtocolHttp)
}
