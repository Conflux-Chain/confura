package metrics

import (
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/metrics/influxdb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func init() {
	if !metrics.Enabled {
		return
	}

	if reportEnabled := viper.GetBool("metrics.report.enabled"); !reportEnabled {
		return
	}

	interval := time.Duration(viper.GetInt64("metrics.report.interval"))

	go influxdb.InfluxDB(
		metrics.DefaultRegistry,
		interval*time.Millisecond,
		viper.GetString("metrics.influxdb.host"),
		viper.GetString("metrics.influxdb.db"),
		viper.GetString("metrics.influxdb.username"),
		viper.GetString("metrics.influxdb.password"),
		"", // namespace
	)

	logrus.Info("Start to report metrics to influxdb periodically")
}
