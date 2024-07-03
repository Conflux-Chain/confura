package config

import (
	"github.com/Conflux-Chain/go-conflux-util/config"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util/blacklist"
	"github.com/Conflux-Chain/confura/util/pprof"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"

	// For go-ethereum v1.0.15, node pkg imports internal/debug pkg which will inits log root
	// with `log.GlogHandler`. If we import node pkg from somewhere else, it will override our
	// custom handler defined within function `adaptGethLogger`.
	_ "github.com/ethereum/go-ethereum/node"
)

// Read system enviroment variables prefixed with "INFURA".
// eg., `INFURA_LOG_LEVEL` will override "log.level" config item from the config file.
const viperEnvPrefix = "infura"

func Init() {
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
