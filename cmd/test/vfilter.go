package test

import (
	"github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/test"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	// virtual filter validation configuration
	vfValidConf test.VFValidConfig

	vfTestCmd = &cobra.Command{
		Use:   "vf",
		Short: "validate if filter changes polled from Virtual-Filter proxy complies with fullnode",
		Run:   startVFTest,
	}
)

func init() {
	// fullnode endpoint
	vfTestCmd.Flags().StringVarP(
		&vfValidConf.FullnodeRpcEndpoint,
		"fn-endpoint", "f", "", "fullnode rpc endpoint used as benchmark",
	)
	vfTestCmd.MarkFlagRequired("fn-endpoint")

	// confura RPC endpoint
	vfTestCmd.Flags().StringVarP(
		&vfValidConf.InfuraRpcEndpoint,
		"infura-endpoint", "u", "", "infura rpc endpoint to be validated against",
	)
	vfTestCmd.MarkFlagRequired("infura-endpoint")

	Cmd.AddCommand(vfTestCmd)
}

func startVFTest(cmd *cobra.Command, args []string) {
	if len(vfValidConf.FullnodeRpcEndpoint) == 0 || len(vfValidConf.InfuraRpcEndpoint) == 0 {
		logrus.Fatal("Fullnode && infura rpc endpoint must be configured for virtual filter validation")
	}

	logrus.Info("Starting virtual filter validator...")

	validator := test.MustNewVFValidator(&vfValidConf)
	defer validator.Destroy()

	util.StartAndGracefulShutdown(validator.Run)
}
