package test

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "test",
	Short: "Start data validity test for JSON-RPC, Pub/Sub or Filter API proxy",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}
