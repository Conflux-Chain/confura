package test

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "test",
	Short: "Test subcommands",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}
