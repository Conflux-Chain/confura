package config

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/ethereum/go-ethereum/log"
	slogrus "github.com/samber/slog-logrus/v2"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func BenchmarkGethDisabledDebug(b *testing.B) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	logger.SetLevel(logrus.InfoLevel)
	handler := newGethLogHandler(logger)
	log := slog.New(handler.WithGroup("geth")).With("conn", "127.0.0.1:1234")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Debug("Served cfx_getLogs", "reqid", 1, "err", "result limit exceeded")
	}
}

func TestGethLogLevelFollowsLogrus(t *testing.T) {
	logger := logrus.New()
	handler := newGethLogHandler(logger).WithGroup("geth").WithAttrs([]slog.Attr{slog.String("conn", "peer")})
	for _, test := range []struct {
		level logrus.Level
		min   slog.Level
	}{
		{logrus.TraceLevel, log.LevelTrace},
		{logrus.DebugLevel, slog.LevelDebug},
		{logrus.InfoLevel, slog.LevelInfo},
		{logrus.WarnLevel, slog.LevelError},
		{logrus.ErrorLevel, slog.LevelError},
		{logrus.FatalLevel, log.LevelCrit},
		{logrus.PanicLevel, log.LevelCrit + 1},
	} {
		t.Run(test.level.String(), func(t *testing.T) {
			logger.SetLevel(test.level)
			for _, level := range []slog.Level{log.LevelTrace, slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError, log.LevelCrit} {
				require.Equal(t, level >= test.min, handler.Enabled(context.Background(), level))
			}
		})
	}
}

func TestDisabledGethLogSkipsConversion(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	logger.SetLevel(logrus.InfoLevel)
	conversions := 0
	handler := slogrus.Option{
		Logger: logger,
		Level:  gethLogLevel{logger: logger},
		Converter: func(bool, func([]string, slog.Attr) slog.Attr, []slog.Attr, []string, *slog.Record) map[string]any {
			conversions++
			return nil
		},
	}.NewLogrusHandler()
	log := slog.New(handler.WithGroup("geth")).With("conn", "peer")
	log.Debug("Served cfx_getLogs", "reqid", 1)
	require.Zero(t, conversions)
	logger.SetLevel(logrus.DebugLevel)
	log.Debug("Served cfx_getLogs", "reqid", 1)
	require.Equal(t, 1, conversions)
}

func TestGethLogPreservesFields(t *testing.T) {
	logger := logrus.New()
	var output bytes.Buffer
	logger.SetOutput(&output)
	logger.SetFormatter(&logrus.JSONFormatter{})
	log := slog.New(newGethLogHandler(logger).WithGroup("geth")).With("conn", "peer")
	log.Info("Served cfx_getLogs", "reqid", 1)
	var entry map[string]any
	require.NoError(t, json.Unmarshal(output.Bytes(), &entry))
	require.Equal(t, "Served cfx_getLogs", entry["msg"])
	require.Equal(t, map[string]any{"conn": "peer", "reqid": float64(1)}, entry["geth"])
}

func TestGethLogPreservesInfoMapping(t *testing.T) {
	// Apply Init's Warn override without removing the adapter's default Info mapping.
	previous := slogrus.LogLevels[log.LevelWarn]
	slogrus.LogLevels[log.LevelWarn] = logrus.InfoLevel
	t.Cleanup(func() { slogrus.LogLevels[log.LevelWarn] = previous })

	logger := logrus.New()
	var output bytes.Buffer
	logger.SetOutput(&output)
	logger.SetFormatter(&logrus.JSONFormatter{})
	gethLogger := log.NewLogger(newGethLogHandler(logger))
	for _, level := range []logrus.Level{logrus.InfoLevel, logrus.WarnLevel, logrus.ErrorLevel} {
		t.Run(level.String(), func(t *testing.T) {
			logger.SetLevel(level)
			output.Reset()
			gethLogger.Info("geth info")
			gethLogger.Warn("geth warn")
			if level != logrus.InfoLevel {
				require.Empty(t, output.String())
				return
			}
			decoder := json.NewDecoder(&output)
			for _, message := range []string{"geth info", "geth warn"} {
				var entry map[string]any
				require.NoError(t, decoder.Decode(&entry))
				require.Equal(t, "info", entry["level"])
				require.Equal(t, message, entry["msg"])
			}
			var extra map[string]any
			require.ErrorIs(t, decoder.Decode(&extra), io.EOF)
		})
	}
}
