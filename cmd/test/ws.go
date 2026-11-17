package test

import (
	"strings"

	"github.com/Conflux-Chain/confura/test"
	"github.com/spf13/cobra"
)

var (
	// Pub/Sub validation configuration
	psConf    test.PSVConfig
	psNetwork string

	wsTestCmd = &cobra.Command{
		Use:   "ws",
		Short: "validate if chain data from websocket Pub/Sub proxy complies with fullnode",
		RunE:  startWSTest,
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

func startWSTest(cmd *cobra.Command, args []string) error {
	if err := validateNetwork(psNetwork); err != nil {
		return err
	}
	if err := validateEndpoints(true, psConf.FullnodeRpcEndpoint, psConf.InfuraRpcEndpoint); err != nil {
		return err
	}
	return runValidator(cmd, func() (test.TestValidator, error) {
		if strings.EqualFold(psNetwork, "eth") {
			return test.NewEthPubSubValidator(&psConf)
		}
		return test.NewCfxPubSubValidator(&psConf)
	})
}
