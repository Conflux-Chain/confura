package config

import (
	"log/slog"

	"github.com/ethereum/go-ethereum/log"
	slogrus "github.com/samber/slog-logrus/v2"
	"github.com/sirupsen/logrus"
)

// gethLogLevel filters records before field conversion; it does not change their
// output level. Init preserves Info -> Info and maps Warn -> Info, so a Logrus
// Info threshold accepts both, while a Warn threshold starts at Geth Error.
type gethLogLevel struct {
	logger *logrus.Logger
}

func (l gethLogLevel) Level() slog.Level {
	switch l.logger.GetLevel() {
	case logrus.TraceLevel:
		return log.LevelTrace
	case logrus.DebugLevel:
		return slog.LevelDebug
	case logrus.InfoLevel:
		return slog.LevelInfo
	case logrus.WarnLevel, logrus.ErrorLevel:
		return slog.LevelError
	case logrus.FatalLevel:
		return log.LevelCrit
	default: // Geth has no panic-level records.
		return log.LevelCrit + 1
	}
}

func newGethLogHandler(logger *logrus.Logger) slog.Handler {
	return slogrus.Option{Logger: logger, Level: gethLogLevel{logger: logger}}.NewLogrusHandler()
}
