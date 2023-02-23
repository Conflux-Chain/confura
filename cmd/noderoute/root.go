package noderoute

import (
	"github.com/spf13/cobra"
)

var (
	Cmd = &cobra.Command{
		Use:   "noderoute",
		Short: "Node route utility toolset",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}
)
