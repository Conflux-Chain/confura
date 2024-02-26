package metrics

import (
	"time"

	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/metrics/influxdb"
	"github.com/sirupsen/logrus"
)

// This package should be imported before any metric (e.g. timer, histogram) created.
// Because, `metrics.Enabled` in go-ethereum is `false` by default, which leads to noop
// metric created for static variables in any package.
//
// In addition, this package should be imported after the initialization of viper and logrus.
func MustInit() {
	var config struct {
		Enabled  bool `default:"true"`
		Influxdb struct {
			Host     string `default:"http://127.0.0.1:8086"`
			DB       string `default:"infura_test"`
			Username string
			Password string
		}
		Report struct {
			Enabled  bool
			Interval time.Duration `default:"10s"`
		}
	}

	viper.MustUnmarshalKey("metrics", &config)

	metrics.Enabled = config.Enabled

	if !metrics.Enabled || !config.Report.Enabled {
		return
	}

	go influxdb.InfluxDB(
		InfuraRegistry,
		config.Report.Interval,
		config.Influxdb.Host,
		config.Influxdb.DB,
		config.Influxdb.Username,
		config.Influxdb.Password,
		"", // namespace
	)

	logrus.Info("Start to report metrics to influxdb periodically")
}
