package test

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/Conflux-Chain/confura/test"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/spf13/cobra"
)

func init() {
	var conf test.LogValidationConfig
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Compare persisted event logs with a healthy fullnode until caught up with the database",
		RunE: func(cmd *cobra.Command, args []string) error {
			var tuning struct {
				BatchSize uint64 `default:"100"`
			}
			viper.MustUnmarshalKey("test.logs", &tuning)
			conf.BatchSize = tuning.BatchSize
			ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
			return test.ValidateLogs(ctx, conf)
		},
	}
	cmd.Flags().StringVar(&conf.Space, "space", "both", "core, espace or both")
	cmd.Flags().Uint64Var(&conf.FromEpoch, "from-epoch", 0, "inclusive Core epoch (also eSpace block number)")
	cmd.MarkFlagRequired("from-epoch")
	cmd.Flags().StringVar(&conf.CoreEndpoint, "core-fn-endpoint", "", "healthy Core fullnode RPC endpoint")
	cmd.Flags().StringVar(&conf.EspaceEndpoint, "espace-fn-endpoint", "", "healthy eSpace fullnode RPC endpoint")
	cmd.Flags().StringVar(&conf.Report, "report", "", "new JSONL file for results and failure context")
	cmd.MarkFlagRequired("report")
	Cmd.AddCommand(cmd)
}
