package cmd

import (
	"os"
	"os/exec"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestCommandFailureExitStatus(t *testing.T) {
	if os.Getenv("CONFURA_COMMAND_CHILD") == "1" {
		viper.Reset()
		// Invalid configuration must not be decoded for pure parameter errors.
		viper.Set("store.mysql.enabled", true)
		viper.Set("store.mysql.dsn", "invalid-dsn")
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{"confura"}, os.Args[i+1:]...)
				break
			}
		}
		Execute()
		return
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"ratelimit", "gk", "-t", "7"}, "invalid rate limit type"},
		{[]string{"ratelimit", "adds", "-n", "cfx", "-a", "test", "-r", "invalid"}, "invalid strategy rules"},
		{[]string{"acl", "addal", "-n", "cfx", "-a", "test", "-r", "invalid"}, "invalid allowlist rules"},
		{[]string{"noderoute", "ls", "-n", "typo"}, "invalid network"},
		{[]string{"noderoute", "ls", "-n", "eth"}, "eth database is unavailable"},
		{[]string{"benchmark", "catchup", "-n", "eth"}, "database is not provided"},
	} {
		t.Run(tc.args[0]+"/"+tc.args[1]+"/"+tc.want, func(t *testing.T) {
			args := append([]string{"-test.run=^TestCommandFailureExitStatus$", "--"}, tc.args...)
			child := exec.Command(os.Args[0], args...)
			child.Env = append(os.Environ(), "CONFURA_COMMAND_CHILD=1")
			out, err := child.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(out))
			require.Equal(t, 1, exit.ExitCode(), string(out))
			require.Contains(t, string(out), tc.want)
			require.NotContains(t, string(out), "invalid database DSN")
		})
	}
}
