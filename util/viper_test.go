package util

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// Stub struct for testing
type logLimit struct {
	MaxLogs int `mapstructure:"maxLogs"`
}

type threshold struct {
	Log logLimit `mapstructure:"log"`
}

type pruneConfig struct {
	Interval  time.Duration `mapstructure:"interval"`
	Threshold threshold     `mapstructure:"threshold"`
}

func TestViperSub(t *testing.T) {
	os.Setenv("CI_PRUNE_THRESHOLD_LOG_MAXLOGS", "1000")

	viper.AutomaticEnv()
	viper.SetEnvPrefix("ci")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	var jsonConf = []byte(`{"prune": {"interval": "1s","threshold": {"log":{"maxLogs": "2000"}}}}`)

	viper.SetConfigType("json")
	err := viper.ReadConfig(bytes.NewBuffer(jsonConf))
	assert.Nil(t, err)

	var pc pruneConfig
	err = viper.Sub("prune").Unmarshal(&pc)
	assert.Nil(t, err)

	assert.NotEqualValues(t, 1000, pc.Threshold.Log.MaxLogs)

	var pc2 pruneConfig
	vsi := ViperSub(viper.GetViper(), "prune")
	err = vsi.Unmarshal(&pc2)
	assert.Nil(t, err)
	assert.EqualValues(t, 1000, pc2.Threshold.Log.MaxLogs)

	os.Setenv("CI_PRUNE_THRESHOLD_LOG_MAXLOGS", "5000")

	var pc3 pruneConfig
	vsi2 := ViperSub(viper.GetViper(), "prune")
	err = vsi2.Unmarshal(&pc3)
	assert.Nil(t, err)

	assert.EqualValues(t, 5000, pc3.Threshold.Log.MaxLogs)

	os.Setenv("CI_PRUNE_THRESHOLD_LOG_MAXLOGS", "15000")

	var llc logLimit
	vsi3 := ViperSub(viper.GetViper(), "prune.threshold.log")
	err = vsi3.Unmarshal(&llc)
	assert.Nil(t, err)

	assert.EqualValues(t, 15000, llc.MaxLogs)
}
