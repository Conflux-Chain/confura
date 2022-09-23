package ratelimit

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "ratelimit",
	Short: "Rate limiting utility toolset",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}
