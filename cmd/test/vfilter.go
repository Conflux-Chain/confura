package test

import (
	"strings"

	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/test"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// virtual filter validation configuration
	vfConf    test.VFValidConfig
	vfNetwork string

	vfTestCmd = &cobra.Command{
		Use:   "vf",
		Short: "validate if filter changes polled from Virtual-Filter proxy complies with fullnode",
		Run:   startVFTest,
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

func startVFTest(cmd *cobra.Command, args []string) {
	if len(vfConf.FullnodeRpcEndpoint) == 0 || len(vfConf.InfuraRpcEndpoint) == 0 {
		logrus.Fatal("Fullnode && infura rpc endpoint must be configured for virtual filter validation")
	}

	validator := mustNewVfTestValidator()
	defer validator.Destroy()

	logrus.Info("Starting virtual filter validator...")
	util.StartAndGracefulShutdown(validator.Run)
}

func mustNewVfTestValidator() test.TestValidator {
	switch {
	case strings.EqualFold(vfNetwork, "eth"):
		return test.MustNewEthVFValidator(&vfConf)
	case strings.EqualFold(vfNetwork, "cfx"):
		return test.MustNewCfxVFValidator(&vfConf)
	default:
		logrus.Fatal("invalid network space (only `cfx` and `eth` acceptable)")
		return nil
	}
}
