package acl

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "acl",
	Short: "Access control utility toolset",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}
