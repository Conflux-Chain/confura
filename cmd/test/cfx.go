package test

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/test"
	"github.com/spf13/cobra"
)

var (
	// core space epoch data validation configuration
	validConf test.EVConfig

	testCmd = &cobra.Command{
		Use:   "cfx",
		Short: "validate if epoch data from core space JSON-RPC proxy complies with fullnode",
		RunE:  startTest,
	}
)

func init() {
	// fullnode endpoint URL
	testCmd.Flags().StringVarP(
		&validConf.FullnodeRpcEndpoint,
		"fn-endpoint", "f", "", "fullnode rpc endpoint used as benchmark",
	)
	testCmd.MarkFlagRequired("fn-endpoint")

	// confura RPC endpoint
	testCmd.Flags().StringVarP(
		&validConf.InfuraRpcEndpoint,
		"infura-endpoint", "u", "", "infura rpc endpoint to be validated against",
	)
	testCmd.MarkFlagRequired("infura-endpoint")

	// start point of epoch
	testCmd.Flags().Uint64VarP(
		&validConf.EpochScanFrom,
		"start-epoch", "e", math.MaxUint64, "the epoch from where scan validation will start",
	)

	// scan interval
	testCmd.Flags().DurationVarP(
		&validConf.ScanInterval,
		"scan-interval", "c", 1*time.Second, "the interval for each scan validation",
	)

	// sampling interval
	testCmd.Flags().DurationVarP(
		&validConf.SamplingInterval,
		"sampling-interval", "a", 10*time.Second, "the interval for each sampling validation",
	)

	// sampling epoch type
	testCmd.Flags().StringVarP(
		&validConf.SamplingEpochType,
		"sampling-epoch-type", "t", "lf", `sampling epoch type: 
			lm(latest_mined)/ls(latest_state)/lc(latest_confirmed)/lf(latest_finalized)/lcp(latest_checkpoint)
		`,
	)

	Cmd.AddCommand(testCmd)
}

func startTest(cmd *cobra.Command, args []string) error {
	if err := validateEndpoints(false, validConf.FullnodeRpcEndpoint, validConf.InfuraRpcEndpoint); err != nil {
		return err
	}
	if err := validateIntervals(validConf.ScanInterval, validConf.SamplingInterval); err != nil {
		return err
	}
	switch strings.ToLower(validConf.SamplingEpochType) {
	case "lm", "ls", "lc", "lf", "lcp", "latest_mined", "latest_state", "latest_confirmed", "latest_finalized", "latest_checkpoint":
	default:
		return fmt.Errorf("unsupported sampling epoch type")
	}
	return runValidator(cmd, func() (test.TestValidator, error) { return test.NewEpochValidator(&validConf) })
}
