package config

import (
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func init() {
	mustInitViper()
	initLogger()
	initMetrics()
}

func mustInitViper() {
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	if err := viper.ReadInConfig(); err != nil {
		panic(errors.WithMessage(err, "Failed to initialize viper"))
	}
}

func initLogger() {
	lvl := viper.GetString("log.level")
	level, err := logrus.ParseLevel(lvl)
	if err != nil {
		logrus.WithError(err).Fatalf("invalid log level configured: %v", lvl)
	}
	logrus.SetLevel(level)
}

func initMetrics() {
	// must initialized before any metrics created to avoid noop metrics created
	metrics.Enabled = viper.GetBool("metrics.enabled")
	if metrics.Enabled {
		logrus.Info("Metrics enabled")
	}
}
