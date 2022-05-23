package test

import (
	"github.com/conflux-chain/conflux-infura/cmd/util"
	"github.com/conflux-chain/conflux-infura/test"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	psValidConf test.PSVConfig // pubsub validation configuration
	wsTestCmd   = &cobra.Command{
		Use:   "ws",
		Short: "Test/validate if infura websocket pubsub complies with fullnode",
		Run:   startWSTest,
	}
)

func init() {
	wsTestCmd.Flags().StringVarP(&psValidConf.FullnodeRpcEndpoint, "fn-endpoint", "f", "", "fullnode rpc endpoint used as benchmark")
	wsTestCmd.MarkFlagRequired("fn-endpoint")

	wsTestCmd.Flags().StringVarP(&psValidConf.InfuraRpcEndpoint, "infura-endpoint", "u", "", "infura rpc endpoint to be validated against")
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
