package config

import (
	"strings"

	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/conflux-chain/conflux-infura/alert"
	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/ethereum/go-ethereum/log"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	// For go-ethereum v1.0.15, node pkg imports internal/debug pkg which will inits
	// log root with a log.GlogHandler.
	// Sadly if we import node pkg somewhere else, it will overrides our custom handler
	// defined within function `adaptGethLogger`.
	_ "github.com/ethereum/go-ethereum/node"
)

// Read system enviroment variables prefixed with "INFURA_"
// eg., INFURA__LOG_LEVEL will override "log.level" config item from config file
const viperEnvPrefix = "infura"

func init() {
	viper.MustInit(viperEnvPrefix)
	initLogger()
	metrics.Init()
	alert.InitDingRobot()
}

func initLogger() {
	var config struct {
		Level string `default:"info"`
	}
	viper.MustUnmarshalKey("log", &config)

	// Set log level
	level, err := logrus.ParseLevel(config.Level)
	if err != nil {
		logrus.WithError(err).Fatalf("invalid log level configured: %v", config.Level)
	}
	logrus.SetLevel(level)

	// Add alert hook for logrus fatal/warn/error level
	hookLevels := []logrus.Level{logrus.FatalLevel, logrus.WarnLevel, logrus.ErrorLevel}
	logrus.AddHook(alert.NewLogrusAlertHook(hookLevels))

	// Customize logger here...
	adaptGethLogger()
}

// adaptGethLogger adapt geth logger (which is used by go sdk) to get along with logrus.
func adaptGethLogger() {
	formatter := log.TerminalFormat(false)
	logrusLevelsMap := map[log.Lvl]logrus.Level{
		log.LvlCrit:  logrus.FatalLevel,
		log.LvlError: logrus.ErrorLevel,
		log.LvlWarn:  logrus.DebugLevel,
		log.LvlInfo:  logrus.DebugLevel,
		log.LvlDebug: logrus.DebugLevel,
		log.LvlTrace: logrus.TraceLevel,
	}

	log.Root().SetHandler(log.FuncHandler(func(r *log.Record) error {
		logLvl, ok := logrusLevelsMap[r.Lvl]
		if !ok {
			return errors.New("unsupported log level")
		}

		if logLvl <= logrus.GetLevel() {
			logStr := string(formatter.Format(r))
			abbrStr := logStr

			firstLineEnd := strings.IndexRune(logStr, '\n')
			if firstLineEnd > 0 { // extract first line as abstract
				abbrStr = logStr[:firstLineEnd]
			}

			logrus.WithField("gethWrappedLogs", logStr).Log(logLvl, abbrStr)
		}

		return nil
	}))
}
