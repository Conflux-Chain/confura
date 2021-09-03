package cmd

import (
	"context"
	"sync"

	"github.com/conflux-chain/conflux-infura/test"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	psValidConf test.PSVConfig // pubsub validation configuration
	wsTestCmd   = &cobra.Command{
		Use:   "wstest",
		Short: "Test/validate if infura websocket pubsub complies with fullnode",
		Run:   startWSTest,
	}
)

func init() {
	wsTestCmd.Flags().StringVarP(&psValidConf.FullnodeRpcEndpoint, "fn-endpoint", "f", "", "fullnode rpc endpoint used as benchmark")
	wsTestCmd.MarkFlagRequired("fn-endpoint")

	wsTestCmd.Flags().StringVarP(&psValidConf.InfuraRpcEndpoint, "infura-endpoint", "u", "", "infura rpc endpoint to be validated against")
	wsTestCmd.MarkFlagRequired("infura-endpoint")
}

func startWSTest(cmd *cobra.Command, args []string) {
	if len(psValidConf.FullnodeRpcEndpoint) == 0 || len(psValidConf.InfuraRpcEndpoint) == 0 {
		logrus.Fatal("Fullnode && infura websocket rpc endpoint must be configured for pubsub test/validation")
	}

	// Context to control child go routines
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	logrus.Info("Starting websocket pubsub validator...")

	validator := test.MustNewPubSubValidator(&psValidConf)
	go validator.Run(ctx, wg)

	gracefulShutdown(ctx, func() error {
		validator.Destroy()
		return nil
	}, wg, cancel)
}
