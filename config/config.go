package config

import (
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util/blacklist"
	"github.com/Conflux-Chain/confura/util/pprof"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/Conflux-Chain/confura/util/rpc/cache"
	"github.com/Conflux-Chain/go-conflux-util/config"
)

// Read system environment variables prefixed with "INFURA".
// eg., `INFURA_LOG_LEVEL` will override "log.level" config item from the config file.
const viperEnvPrefix = "infura"

func Init() {
	// init utilities eg., viper, alert, metrics and logging
	config.MustInit(viperEnvPrefix)

	// init pprof
	pprof.MustInit()

	// init misc util
	cache.MustInitFromViper()
	rpcutil.MustInit()
	blacklist.MustInit()

	// init store
	store.MustInit()
	// init node
	node.MustInit()
	// init rpc
	rpc.MustInit()
}
