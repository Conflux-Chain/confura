package cmd

import (
	"context"
	"sync"

	"github.com/conflux-chain/conflux-infura/test"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	testFnRpcEndpoint     string // fullnode rpc endpoint for data test/validation
	testInfuraRpcEndpoint string // infura rpc endpoint for data test/validation

	testCmd = &cobra.Command{
		Use:   "test",
		Short: "Test/validate if infura data complies with fullnode",
		Run:   startTest,
	}
)

func init() {
	testCmd.Flags().StringVarP(&testFnRpcEndpoint, "fn-endpoint", "f", "", "fullnode rpc endpoint used as benchmark")
	testCmd.MarkFlagRequired("fn-endpoint")

	testCmd.Flags().StringVarP(&testInfuraRpcEndpoint, "infura-endpoint", "u", "", "infura rpc endpoint to be validated against")
	testCmd.MarkFlagRequired("infura-endpoint")
}

func startTest(cmd *cobra.Command, args []string) {
	if len(testFnRpcEndpoint) == 0 || len(testInfuraRpcEndpoint) == 0 {
		logrus.Fatal("Fullnode && infura rpc endpoint must be configured for data test/validation")
	}

	// Context to control child go routines
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	logrus.Info("Starting epoch data validator...")

	validator := test.MustNewEpochValidator(&test.EVConfig{
		FullnodeRpcEndpoint: testFnRpcEndpoint,
		InfuraRpcEndpoint:   testInfuraRpcEndpoint,
	})
	go validator.Run(ctx, wg)

	gracefulShutdown(ctx, func() error {
		validator.Destroy()
		return nil
	}, wg, cancel)
}
