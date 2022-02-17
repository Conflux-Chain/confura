package test

import (
	"math"
	"time"

	"github.com/conflux-chain/conflux-infura/test"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	ethValidConf test.EthValidConfig // eth data validation configuration
	ethTestCmd   = &cobra.Command{
		Use:   "eth",
		Short: "Test/validate if infura ETH data complies with fullnode",
		Run:   startEthTest,
	}
)

func init() {
	ethTestCmd.Flags().StringVarP(
		&ethValidConf.FullnodeRpcEndpoint, "fn-endpoint", "f", "", "fullnode rpc endpoint used as benchmark",
	)
	ethTestCmd.MarkFlagRequired("fn-endpoint")

	ethTestCmd.Flags().StringVarP(
		&ethValidConf.InfuraRpcEndpoint, "infura-endpoint", "u", "", "infura rpc endpoint to be validated against",
	)
	ethTestCmd.MarkFlagRequired("infura-endpoint")

	ethTestCmd.Flags().Uint64VarP(
		&ethValidConf.ScanFromBlock, "start-block", "b", math.MaxUint64, "the block from where scan validation will start",
	)

	ethTestCmd.Flags().DurationVarP(
		&ethValidConf.ScanInterval, "scan-interval", "c", 1*time.Second, "the interval for each scan validation",
	)

	ethTestCmd.Flags().DurationVarP(
		&ethValidConf.SamplingInterval, "sampling-interval", "a", 15*time.Second, "the interval for each sampling validation",
	)

	Cmd.AddCommand(ethTestCmd)
}

func startEthTest(cmd *cobra.Command, args []string) {
	if len(ethValidConf.FullnodeRpcEndpoint) == 0 || len(ethValidConf.InfuraRpcEndpoint) == 0 {
		logrus.Fatal("Fullnode && infura rpc endpoint must be configured for ETH data test/validation")
	}

	logrus.Info("Starting ETH data validator...")

	validator := test.MustNewEthValidator(&ethValidConf)
	defer validator.Destroy()
	util.StartAndGracefulShutdown(validator.Run)
}
