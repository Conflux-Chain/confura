package util

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, errors.New("broken input") }

func TestConfirmWrite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   io.Reader
		yes, ok bool
	}{
		{"EOF", strings.NewReader(""), false, false},
		{"read error", brokenReader{}, false, false},
		{"text", strings.NewReader("yes\n"), false, false},
		{"unterminated", strings.NewReader(" "), false, false},
		{"Enter", strings.NewReader("\n"), false, true},
		{"CRLF", strings.NewReader("\r\n"), false, true},
		{"yes flag", brokenReader{}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			AddConfirmationFlag(cmd)
			if tc.yes {
				require.NoError(t, cmd.Flags().Set("yes", "true"))
			}
			cmd.SetIn(tc.input)
			var out bytes.Buffer
			cmd.SetOut(&out)
			err := ConfirmWrite(cmd, "eth", "delete", "fixture")
			require.Equal(t, tc.ok, err == nil)
			require.Contains(t, out.String(), "eth")
			require.Contains(t, out.String(), "fixture")
		})
	}
}
