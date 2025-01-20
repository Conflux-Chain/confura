package benchmark

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Start benchmark testing for catch-up sync etc.",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}
