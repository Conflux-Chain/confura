package config

import (
	"strings"

	"github.com/conflux-chain/conflux-infura/alert"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func init() {
	mustInitViper()
	initLogger()
	initMetrics()
	initAlert()
}

func mustInitViper() {
	// Read system enviroment variables prefixed with "INFURA_"
	// eg., INFURA__LOG_LEVEL will override "log.level" config item from config file
	viper.AutomaticEnv()
	viper.SetEnvPrefix("infura")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	if err := viper.ReadInConfig(); err != nil {
		panic(errors.WithMessage(err, "Failed to initialize viper"))
	}
}

func initAlert() {
	if viper.GetBool("alert.dingtalk.enabled") {
		alert.InitDingRobot()
	}
}

func initLogger() {
	lvl := viper.GetString("log.level")
	level, err := logrus.ParseLevel(lvl)
	if err != nil {
		logrus.WithError(err).Fatalf("invalid log level configured: %v", lvl)
	}
	logrus.SetLevel(level)

	// Add alert hook for logrus fatal/warn/error level
	hookLevels := []logrus.Level{logrus.FatalLevel, logrus.WarnLevel, logrus.ErrorLevel}
	logrus.AddHook(alert.NewLogrusAlertHook(hookLevels))
}

func initMetrics() {
	// must initialized before any metrics created to avoid noop metrics created
	metrics.Enabled = viper.GetBool("metrics.enabled")
	if metrics.Enabled {
		logrus.Info("Metrics enabled")
	}
}
