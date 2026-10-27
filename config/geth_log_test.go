package config

import (
	"io"
	"log/slog"
	"testing"

	slogrus "github.com/samber/slog-logrus/v2"
	"github.com/sirupsen/logrus"
)

func BenchmarkGethDisabledDebug(b *testing.B) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	logger.SetLevel(logrus.InfoLevel)
	handler := slogrus.Option{Logger: logger}.NewLogrusHandler()
	log := slog.New(handler.WithGroup("geth")).With("conn", "127.0.0.1:1234")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Debug("Served cfx_getLogs", "reqid", 1, "err", "result limit exceeded")
	}
}
