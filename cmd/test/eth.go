package test

import (
	"math"
	"time"

	"github.com/scroll-tech/rpc-gateway/cmd/util"
	"github.com/scroll-tech/rpc-gateway/test"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// evm space block data validation configuration
	ethValidConf test.EthValidConfig

	ethTestCmd = &cobra.Command{
		Use:   "eth",
		Short: "validate if epoch data from evm space JSON-RPC proxy complies with fullnode",
		Run:   startEthTest,
	}
)

func init() {
	// fullnode endpoint
	ethTestCmd.Flags().StringVarP(
		&ethValidConf.FullnodeRpcEndpoint,
		"fn-endpoint", "f", "", "fullnode rpc endpoint used as benchmark",
	)
	ethTestCmd.MarkFlagRequired("fn-endpoint")

	// confura RPC endpoint
	ethTestCmd.Flags().StringVarP(
		&ethValidConf.InfuraRpcEndpoint,
		"infura-endpoint", "u", "", "infura rpc endpoint to be validated against",
	)
	ethTestCmd.MarkFlagRequired("infura-endpoint")

	// start point of block
	ethTestCmd.Flags().Uint64VarP(
		&ethValidConf.ScanFromBlock,
		"start-block", "b", math.MaxUint64, "the block from where scan validation will start",
	)

	// scan interval
	ethTestCmd.Flags().DurationVarP(
		&ethValidConf.ScanInterval,
		"scan-interval", "c", 1*time.Second, "the interval for each scan validation",
	)

	// sampling interval
	ethTestCmd.Flags().DurationVarP(
		&ethValidConf.SamplingInterval,
		"sampling-interval", "a", 15*time.Second, "the interval for each sampling validation",
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
