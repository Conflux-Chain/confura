package rpc

import (
	"context"
	"time"

	"github.com/conflux-chain/conflux-infura/util/metrics"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/sirupsen/logrus"
)

func middlewareMetrics(
	fullnode, space string,
	handler func(ctx context.Context, result interface{}, method string, args ...interface{}) error,
) func(ctx context.Context, result interface{}, method string, args ...interface{}) error {
	return func(ctx context.Context, result interface{}, method string, args ...interface{}) error {
		start := time.Now()

		err := handler(ctx, result, method, args...)

		metrics.Registry.RPC.FullnodeQps(space, method, err).UpdateSince(start)

		// overall error rate for each full node
		metrics.Registry.RPC.FullnodeErrorRate().Mark(err != nil)
		metrics.Registry.RPC.FullnodeErrorRate(fullnode).Mark(err != nil)
		nonRpcErr := err != nil && !utils.IsRPCJSONError(err) // generally io error
		metrics.Registry.RPC.FullnodeNonRpcErrorRate().Mark(nonRpcErr)
		metrics.Registry.RPC.FullnodeNonRpcErrorRate(fullnode).Mark(nonRpcErr)

		return err
	}
}

func middlewareLog(
	fullnode, space string,
	handler func(ctx context.Context, result interface{}, method string, args ...interface{}) error,
) func(ctx context.Context, result interface{}, method string, args ...interface{}) error {
	return func(ctx context.Context, result interface{}, method string, args ...interface{}) error {
		if !logrus.IsLevelEnabled(logrus.DebugLevel) {
			return handler(ctx, result, method, args...)
		}

		logger := logrus.WithFields(logrus.Fields{
			"fullnode": fullnode,
			"space":    space,
			"method":   method,
			"args":     args,
		})

		logger.Debug("RPC enter")

		start := time.Now()
		err := handler(ctx, result, method, args...)
		logger = logger.WithField("elapsed", time.Since(start))

		if err != nil {
			logger = logger.WithError(err)
		}

		if logrus.IsLevelEnabled(logrus.TraceLevel) {
			logger = logger.WithField("result", result)
		}

		logger.Debug("RPC leave")

		return err
	}
}
