package virtualfilter

import (
	"time"

	"github.com/Conflux-Chain/confura/util/viper"
)

const defaultMaxFilterChainNodes = 100000

// ethConfig represents the configuration of the EVM space virtual filter system.
type ethConfig struct {
	Endpoint string        `default:":48545"` // server listening endpoint (default: :48545)
	TTL      time.Duration `default:"1m"`     // how long filters stay active (default: 1min)

	// max number of filter blocks full of event logs to restrict memory usage (default: 100)
	MaxFullFilterBlocks int `default:"100"`

	// Max retained canonical and fork nodes. Exceeding it invalidates the session.
	MaxFilterChainNodes int `default:"100000"`
}

func mustNewEthConfigFromViper() *ethConfig {
	var conf ethConfig
	viper.MustUnmarshalKey("ethVirtualFilters", &conf)

	if conf.MaxFilterChainNodes <= 0 {
		panic("MaxFilterChainNodes must be positive")
	}
	return &conf
}

// cfxConfig represents the configuration of the core space virtual filter system.
type cfxConfig struct {
	Endpoint string        `default:":42537"` // server listening endpoint (default: :42537)
	TTL      time.Duration `default:"1m"`     // how long filters stay active (default: 1min)

	// max number of filter epochs full of event logs to restrict memory usage (default: 100)
	MaxFullFilterEpochs int `default:"100"`

	// Max retained canonical and fork nodes. Exceeding it invalidates the session.
	MaxFilterChainNodes int `default:"100000"`
}

func mustNewCfxConfigFromViper() *cfxConfig {
	var conf cfxConfig
	viper.MustUnmarshalKey("virtualFilters", &conf)

	if conf.MaxFilterChainNodes <= 0 {
		panic("MaxFilterChainNodes must be positive")
	}
	return &conf
}
