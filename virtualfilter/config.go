package virtualfilter

import (
	"time"

	"github.com/Conflux-Chain/go-conflux-util/viper"
)

// ethConfig represents the configuration of the EVM space virtual filter system.
type ethConfig struct {
	Endpoint string        `default:":48545"` // server listening endpoint (default: :48545)
	TTL      time.Duration `default:"1m"`     // how long filters stay active (default: 1min)

	// max number of filter blocks full of event logs to restrict memory usage (default: 100)
	MaxFullFilterBlocks int `default:"100"`
}

func mustNewEthConfigFromViper() *ethConfig {
	var conf ethConfig
	viper.MustUnmarshalKey("ethVirtualFilters", &conf)

	return &conf
}

// cfxConfig represents the configuration of the core space virtual filter system.
type cfxConfig struct {
	Endpoint string        `default:":42537"` // server listening endpoint (default: :42537)
	TTL      time.Duration `default:"1m"`     // how long filters stay active (default: 1min)

	// max number of filter epochs full of event logs to restrict memory usage (default: 100)
	MaxFullFilterEpochs int `default:"100"`
}

func mustNewCfxConfigFromViper() *cfxConfig {
	var conf cfxConfig
	viper.MustUnmarshalKey("virtualFilters", &conf)

	return &conf
}
