package pprof

import (
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
)

// This package should be imported after the initialization of viper and logrus.
func MustInit() {
	var config struct {
		Enabled      bool
		HttpEndpoint string `default:":6060"`
	}

	viper.MustUnmarshalKey("pprof", &config)
	if !config.Enabled {
		return
	}

	l, err := net.Listen("tcp", config.HttpEndpoint)
	if err != nil {
		logrus.WithError(err).
			WithField("endpoint", config.HttpEndpoint).
			Fatal("Failed to listen http endpoint for pprof")
	}

	go func() {
		logrus.WithField("endpoint", config.HttpEndpoint).
			Info("Start to collect runtime profiling data...")

		defer l.Close()
		http.Serve(l, nil)
	}()
}
