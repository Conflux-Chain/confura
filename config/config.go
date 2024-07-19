package config

import (
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util/blacklist"
	"github.com/Conflux-Chain/confura/util/pprof"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/Conflux-Chain/go-conflux-util/config"
	metricUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/ethereum/go-ethereum/metrics"
)

// Read system enviroment variables prefixed with "INFURA".
// eg., `INFURA_LOG_LEVEL` will override "log.level" config item from the config file.
const viperEnvPrefix = "infura"

func Init() {
	// Note, must use metrics.DefaultRegistry from geth, since go-rpc-provider depends on it
	// for rpc metrics by default. When RPC middleware supported at server side, we can use
	// a custom metrics registry.
	metricUtil.DefaultRegistry = metrics.DefaultRegistry

	// init utilities eg., viper, alert, metrics and logging
	config.MustInit(viperEnvPrefix)

	// init pprof
	pprof.MustInit()

	// init misc util
	rpcutil.MustInit()
	blacklist.MustInit()

	// init store
	store.MustInit()
	// init node
	node.MustInit()
	// init rpc
	rpc.MustInit()
}
