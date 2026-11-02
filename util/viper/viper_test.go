package viper

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	base "github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfigurationContract(t *testing.T) {
	base.Reset()
	t.Cleanup(base.Reset)
	dir := t.TempDir()
	file := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(file, []byte("fixture:\n  count: 0\n  enabled: false\n  names: [explicit]\n"), 0600))
	t.Setenv("UTILFIX_FIXTURE_DELAY", "2s")
	require.NoError(t, Init("UTILFIX", file))
	var conf struct {
		Count   int           `default:"3"`
		Enabled bool          `default:"true"`
		Names   []string      `default:"[one,two]"`
		Delay   time.Duration `default:"1s"`
	}
	require.NoError(t, UnmarshalKey("fixture", &conf))
	require.Zero(t, conf.Count)
	require.False(t, conf.Enabled)
	require.Equal(t, []string{"explicit", "two"}, conf.Names)
	require.Equal(t, 2*time.Second, conf.Delay)
}

func TestMissingConfigAndDotenv(t *testing.T) {
	base.Reset()
	t.Cleanup(base.Reset)
	dir := t.TempDir()
	old, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() { _ = os.Chdir(old) })
	require.NoError(t, os.WriteFile(".env", []byte("UTILFIX_FROM_DOTENV=unexpected\n"), 0600))
	require.Error(t, Init("UTILFIX"))
	require.Empty(t, os.Getenv("UTILFIX_FROM_DOTENV"))
	require.NoError(t, os.WriteFile("config.yaml", []byte("{}"), 0600))
	require.NoError(t, Init("UTILFIX"))
	require.Empty(t, os.Getenv("UTILFIX_FROM_DOTENV"))
}
