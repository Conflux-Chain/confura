package util

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func AddConfirmationFlag(cmd *cobra.Command) {
	cmd.Flags().Bool("yes", false, "Confirm the write without interactive input")
}

// ConfirmWrite accepts only a complete empty line. EOF is never consent.
func ConfirmWrite(cmd *cobra.Command, network, action string, target interface{}) error {
	display, err := json.MarshalIndent(target, "", "  ")
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Network: %s\nOperation: %s\nTarget: %s\n", network, action, display); err != nil {
		return err
	}
	yes, err := cmd.Flags().GetBool("yes")
	if err != nil || yes {
		return err
	}
	if _, err := fmt.Fprintln(cmd.OutOrStdout(), "Press Enter to confirm, or use --yes for automation:"); err != nil {
		return err
	}
	line, err := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
	if err != nil {
		return fmt.Errorf("confirmation not received: %w", err)
	}
	if strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r") != "" {
		return fmt.Errorf("operation canceled: confirmation must be an empty line")
	}
	return nil
}
