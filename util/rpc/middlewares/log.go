package middlewares

import (
	"context"
	"time"

	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

func LogBatch(next rpc.HandleBatchFunc) rpc.HandleBatchFunc {
	return func(ctx context.Context, msgs []*rpc.JsonRpcMessage) []*rpc.JsonRpcMessage {
		if !logrus.IsLevelEnabled(logrus.DebugLevel) {
			return next(ctx, msgs)
		}

		logrus.WithField("batch", len(msgs)).Debug("Batch RPC enter")

		start := time.Now()
		resp := next(ctx, msgs)

		logrus.WithFields(logrus.Fields{
			"batch":   len(resp),
			"elapsed": time.Since(start),
		}).Debug("Batch RPC leave")

		return resp
	}
}

func Log(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		if !logrus.IsLevelEnabled(logrus.DebugLevel) {
			return next(ctx, msg)
		}

		logger := logrus.WithField("input", msg)
		logger.Debug("RPC enter")

		start := time.Now()
		resp := next(ctx, msg)
		logger = logger.WithField("elapsed", time.Since(start))

		if resp.Error != nil {
			logger = logger.WithField(logrus.ErrorKey, resp.Error.Error())
		} else if logrus.IsLevelEnabled(logrus.TraceLevel) {
			logger = logger.WithField("output", resp)
		}

		logger.Debug("RPC leave")

		return resp
	}
}
