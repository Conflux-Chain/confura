package ratelimit

import (
	"github.com/spf13/cobra"
)

var (
	addKeyCmd = &cobra.Command{
		Use:   "addk",
		Short: "Add rate limit key",
		Run:   addKey,
	}

	delKeyCmd = &cobra.Command{
		Use:   "delk",
		Short: "Delete rate limit key",
		Run:   delKey,
	}

	updateKeyCmd = &cobra.Command{
		Use:   "updk",
		Short: "Update rate limit key",
		Run:   updateKey,
	}

	listKeysCmd = &cobra.Command{
		Use:   "lsk",
		Short: "List rate limit keys",
		Run:   listKeys,
	}
)

func init() {
	Cmd.AddCommand(addKeyCmd)
	Cmd.AddCommand(delKeyCmd)
	Cmd.AddCommand(updateKeyCmd)
	Cmd.AddCommand(listKeysCmd)
}

func addKey(cmd *cobra.Command, args []string) {
	// TODO
}

func delKey(cmd *cobra.Command, args []string) {
	// TODO
}

func updateKey(cmd *cobra.Command, args []string) {
	// TODO
}

func listKeys(cmd *cobra.Command, args []string) {
	// TODO
}
