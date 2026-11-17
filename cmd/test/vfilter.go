package test

import (
	"strings"

	"github.com/Conflux-Chain/confura/test"
	"github.com/spf13/cobra"
)

var (
	// virtual filter validation configuration
	vfConf    test.VFValidConfig
	vfNetwork string

	vfTestCmd = &cobra.Command{
		Use:   "vf",
		Short: "validate if filter changes polled from Virtual-Filter proxy complies with fullnode",
		RunE:  startVFTest,
	}
)

func init() {
	// network space
	vfTestCmd.Flags().StringVarP(
		&vfNetwork, "network", "n", "cfx", "network space ('cfx' or 'eth')",
	)
	vfTestCmd.MarkFlagRequired("network")

	// fullnode endpoint
	vfTestCmd.Flags().StringVarP(
		&vfConf.FullnodeRpcEndpoint,
		"fn-endpoint", "f", "", "fullnode rpc endpoint used as benchmark",
	)
	vfTestCmd.MarkFlagRequired("fn-endpoint")

	// confura RPC endpoint
	vfTestCmd.Flags().StringVarP(
		&vfConf.InfuraRpcEndpoint,
		"infura-endpoint", "u", "", "infura rpc endpoint to be validated against",
	)
	vfTestCmd.MarkFlagRequired("infura-endpoint")

	Cmd.AddCommand(vfTestCmd)
}

func startVFTest(cmd *cobra.Command, args []string) error {
	if err := validateNetwork(vfNetwork); err != nil {
		return err
	}
	if err := validateEndpoints(false, vfConf.FullnodeRpcEndpoint, vfConf.InfuraRpcEndpoint); err != nil {
		return err
	}
	return runValidator(cmd, func() (test.TestValidator, error) {
		if strings.EqualFold(vfNetwork, "eth") {
			return test.NewEthVFValidator(&vfConf)
		}
		return test.NewCfxVFValidator(&vfConf)
	})
}
