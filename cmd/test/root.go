package test

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "test",
	Short: "Start data validity test for JSON-RPC and Pub/Sub proxy including core space and evm space",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}
