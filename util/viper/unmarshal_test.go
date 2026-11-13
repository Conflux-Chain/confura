package viper

import (
	"strings"
	"testing"
	"time"

	upstream "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

type testConfig struct {
	Enabled bool          `default:"true"`
	Limit   int           `default:"20"`
	Name    string        `default:"fallback"`
	Timeout time.Duration `default:"3s"`
	Hosts   []string      `default:"[one,two]"`
}

func initConfig(t *testing.T) {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Chdir(t.TempDir()) // Do not load a developer's .env or config file.
	upstream.MustInit("INFURA")
}

func TestUnmarshalKeyDefaultsAndExplicitZero(t *testing.T) {
	initConfig(t)
	var defaults testConfig
	require.NoError(t, UnmarshalKey("service", &defaults))
	require.True(t, defaults.Enabled)
	require.Equal(t, 20, defaults.Limit)
	require.Equal(t, "fallback", defaults.Name)
	require.Equal(t, 3*time.Second, defaults.Timeout)

	viper.SetConfigType("yaml")
	require.NoError(t, viper.ReadConfig(strings.NewReader("service:\n  enabled: false\n  limit: 0\n  name: ''\n  timeout: 0s\n  hosts: [configured]\n")))
	var got testConfig
	require.NoError(t, UnmarshalKey("service", &got))
	require.False(t, got.Enabled)
	require.Zero(t, got.Limit)
	require.Empty(t, got.Name)
	require.Zero(t, got.Timeout)
	require.Equal(t, []string{"configured"}, got.Hosts)
}

func TestUnmarshalKeyEnvironmentAndResolver(t *testing.T) {
	initConfig(t)
	viper.SetConfigType("yaml")
	require.NoError(t, viper.ReadConfig(strings.NewReader("service:\n  enabled: true\n  limit: 12\n")))
	t.Setenv("INFURA_SERVICE_ENABLED", "false")
	t.Setenv("INFURA_SERVICE_LIMIT", "0")
	t.Setenv("INFURA_SERVICE_TIMEOUT", "250ms")
	t.Setenv("INFURA_SERVICE_HOSTS", "env-one,env-two")
	var got testConfig
	require.NoError(t, UnmarshalKey("service", &got))
	require.False(t, got.Enabled)
	require.Zero(t, got.Limit)
	require.Equal(t, 250*time.Millisecond, got.Timeout)
	require.Equal(t, []string{"env-one", "env-two"}, got.Hosts)
	viper.Set("service.limit", 7)
	require.NoError(t, UnmarshalKey("service", &got))
	require.Equal(t, 7, got.Limit, "explicit Viper override retains its precedence over env")
	require.NoError(t, UnmarshalKey("service", &got, func(key string) (interface{}, bool) {
		if key == "service.limit" {
			return 0, true
		}
		if key == "service.hosts" {
			return []string{"resolved"}, true
		}
		return nil, false
	}))
	require.Zero(t, got.Limit)
	require.Equal(t, []string{"resolved"}, got.Hosts)
}

func TestUnmarshalKeyDecodeError(t *testing.T) {
	initConfig(t)
	viper.Set("service.timeout", "invalid duration")
	var got testConfig
	require.Error(t, UnmarshalKey("service", &got))
}
