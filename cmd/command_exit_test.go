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
		{[]string{"test", "cfx", "-f", "http://127.0.0.1:1", "-u", "http://127.0.0.1:1", "--scan-interval", "0s"}, "intervals must be positive"},
		{[]string{"test", "eth", "-f", "http://127.0.0.1:1", "-u", "http://127.0.0.1:1", "--sampling-interval", "-1s"}, "intervals must be positive"},
		{[]string{"test", "cfx", "-f", "http://127.0.0.1:1", "-u", "http://127.0.0.1:1", "-t", "typo"}, "unsupported sampling epoch type"},
		{[]string{"test", "ws", "-n", "typo", "-f", "ws://127.0.0.1:1", "-u", "ws://127.0.0.1:1"}, "network must be cfx or eth"},
		{[]string{"test", "ws", "-n", "eth", "-f", "http://127.0.0.1:1", "-u", "ws://127.0.0.1:1"}, "subscription endpoints require ws or wss"},
		{[]string{"test", "vf", "-n", "cfx", "-f", "ws://127.0.0.1:1", "-u", "http://127.0.0.1:1"}, "RPC endpoints require http or https"},
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
