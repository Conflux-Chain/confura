package pprof

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/Conflux-Chain/confura/util/viper"
	"github.com/sirupsen/logrus"
)

var server *http.Server // initialized once, before command execution

func MustInit() {
	var err error
	server, err = StartFromViper()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to initialize pprof")
	}
}

func Close() error {
	if server != nil {
		return server.Close()
	}
	return nil
}

// StartFromViper binds synchronously so a conflict is a startup error.
func StartFromViper() (*http.Server, error) {
	var cfg struct {
		Enabled                bool
		Endpoint, HttpEndpoint string
	}
	if err := viper.UnmarshalKey("pprof", &cfg); err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.Endpoint != "" && cfg.HttpEndpoint != "" && cfg.Endpoint != cfg.HttpEndpoint {
		return nil, fmt.Errorf("pprof.endpoint and pprof.httpEndpoint conflict")
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = cfg.HttpEndpoint
	}
	if endpoint == "" {
		endpoint = "127.0.0.1:6060"
	}
	l, err := net.Listen("tcp", endpoint)
	if err != nil {
		return nil, fmt.Errorf("listen pprof on %s: %w", endpoint, err)
	}
	srv := &http.Server{Addr: l.Addr().String()}
	go func() {
		if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logrus.WithError(err).Error("pprof server stopped")
		}
	}()
	return srv, nil
}
