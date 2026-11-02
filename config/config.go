package config

import (
	"log/slog"
	"maps"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util/blacklist"
	"github.com/Conflux-Chain/confura/util/pprof"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/Conflux-Chain/confura/util/rpc/cache"
	"github.com/Conflux-Chain/confura/util/viper"
	"github.com/Conflux-Chain/go-conflux-util/alert"
	logutil "github.com/Conflux-Chain/go-conflux-util/log"
	metricsutil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/ethereum/go-ethereum/log"
	slogrus "github.com/samber/slog-logrus/v2"
	"github.com/sirupsen/logrus"
)

// Read system environment variables prefixed with "INFURA".
// eg., `INFURA_LOG_LEVEL` will override "log.level" config item from the config file.
const viperEnvPrefix = "infura"

func Init() {
	// init utilities eg., viper, alert, metrics and logging
	viper.MustInit(viperEnvPrefix)
	var metricConfig metricsutil.Config
	viper.MustUnmarshalKey("metrics", &metricConfig)
	metricsutil.Init(metricConfig)
	alert.MustInitFromViper()
	var logConfig logutil.LoggingConfig
	viper.MustUnmarshalKey("log", &logConfig)
	logutil.MustInit(logConfig)

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

	// Adapt Geth's slog-based logging (used by `go-rpc-provider`) to logrus.
	maps.Copy(slogrus.LogLevels, map[slog.Level]logrus.Level{
		log.LevelCrit:  logrus.FatalLevel,
		log.LevelTrace: logrus.TraceLevel,
		log.LevelWarn:  logrus.InfoLevel,
	})
	handler := newGethLogHandler(logrus.StandardLogger())
	log.SetDefault(log.NewLogger(handler.WithGroup("geth")))
}
