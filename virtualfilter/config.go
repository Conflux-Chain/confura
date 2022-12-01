package virtualfilter

import (
	"time"

	"github.com/Conflux-Chain/go-conflux-util/viper"
)

// Config represents the configuration of the  virtual filter system.
type Config struct {
	Endpoint string        `default:":48545"` // server listening endpoint (default: :48545)
	TTL      time.Duration `default:":1m"`    // how long filters stay active (default: 1min)
	// max number of filter blocks full of event logs to restrict memory usage (default: 100)
	MaxFullFilterBlocks int `default:"100"`
}

func mustNewConfigFromViper() *Config {
	var conf Config
	viper.MustUnmarshalKey("virtualFilters", &conf)

	return &conf
}
