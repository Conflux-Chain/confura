package test

import (
	"strings"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/test"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// Pub/Sub validation configuration
	psConf    test.PSVConfig
	psNetwork string

	wsTestCmd = &cobra.Command{
		Use:   "ws",
		Short: "validate if chain data from websocket Pub/Sub proxy complies with fullnode",
		Run:   startWSTest,
	}
)

func init() {
	// network space
	wsTestCmd.Flags().StringVarP(
		&psNetwork, "network", "n", "cfx", "network space ('cfx' or 'eth')",
	)
	wsTestCmd.MarkFlagRequired("network")

	// fullnode endpoint
	wsTestCmd.Flags().StringVarP(
		&psConf.FullnodeRpcEndpoint,
		"fn-endpoint", "f", "", "fullnode rpc endpoint used as benchmark",
	)
	wsTestCmd.MarkFlagRequired("fn-endpoint")

	// confura RPC endpoint
	wsTestCmd.Flags().StringVarP(
		&psConf.InfuraRpcEndpoint,
		"infura-endpoint", "u", "", "infura rpc endpoint to be validated against",
	)
	wsTestCmd.MarkFlagRequired("infura-endpoint")

	Cmd.AddCommand(wsTestCmd)
}

func startWSTest(cmd *cobra.Command, args []string) {
	if len(psConf.FullnodeRpcEndpoint) == 0 || len(psConf.InfuraRpcEndpoint) == 0 {
		logrus.Fatal("Fullnode && infura websocket rpc endpoint must be configured for pubsub test/validation")
	}

	validator := mustNewWSTestValidator()
	defer validator.Destroy()

	logrus.Info("Starting websocket pubsub validator...")
	util.StartAndGracefulShutdown(validator.Run)
}

func mustNewWSTestValidator() test.TestValidator {
	switch {
	case strings.EqualFold(psNetwork, "eth"):
		return test.MustNewEthPubSubValidator(&psConf)
	case strings.EqualFold(psNetwork, "cfx"):
		return test.MustNewCfxPubSubValidator(&psConf)
	default:
		logrus.Fatal("invalid network space (only `cfx` and `eth` acceptable)")
		return nil
	}
}
