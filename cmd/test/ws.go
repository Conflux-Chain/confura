package test

import (
	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/test"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// core space Pub/Sub validation configuration
	psValidConf test.PSVConfig

	wsTestCmd = &cobra.Command{
		Use:   "ws",
		Short: "validate if epoch data from core space websocket Pub/Sub proxy complies with fullnode",
		Run:   startWSTest,
	}
)

func init() {
	// fullnode endpoint
	wsTestCmd.Flags().StringVarP(
		&psValidConf.FullnodeRpcEndpoint,
		"fn-endpoint", "f", "", "fullnode rpc endpoint used as benchmark",
	)
	wsTestCmd.MarkFlagRequired("fn-endpoint")

	// confura RPC endpoint
	wsTestCmd.Flags().StringVarP(
		&psValidConf.InfuraRpcEndpoint,
		"infura-endpoint", "u", "", "infura rpc endpoint to be validated against",
	)
	wsTestCmd.MarkFlagRequired("infura-endpoint")

	Cmd.AddCommand(wsTestCmd)
}

func startWSTest(cmd *cobra.Command, args []string) {
	if len(psValidConf.FullnodeRpcEndpoint) == 0 || len(psValidConf.InfuraRpcEndpoint) == 0 {
		logrus.Fatal("Fullnode && infura websocket rpc endpoint must be configured for pubsub test/validation")
	}

	logrus.Info("Starting websocket pubsub validator...")

	validator := test.MustNewPubSubValidator(&psValidConf)
	defer validator.Destroy()

	util.StartAndGracefulShutdown(validator.Run)
}
