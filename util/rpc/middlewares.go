package rpc

import (
	"context"
	"strings"
	"time"

	"github.com/conflux-chain/conflux-infura/util/metrics"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/sirupsen/logrus"
)

func Url2NodeName(url string) string {
	nodeName := strings.ToLower(url)
	nodeName = strings.TrimPrefix(nodeName, "http://")
	nodeName = strings.TrimPrefix(nodeName, "https://")
	nodeName = strings.TrimPrefix(nodeName, "ws://")
	nodeName = strings.TrimPrefix(nodeName, "wss://")
	return strings.TrimPrefix(nodeName, "/")
}

func HookMiddlewares(provider *providers.MiddlewarableProvider, url, space string) {
	nodeName := Url2NodeName(url)
	provider.HookCallContext(middlewareLog(nodeName, space))
	provider.HookCallContext(middlewareMetrics(nodeName, space))
}

func middlewareMetrics(fullnode, space string) providers.CallContextMiddleware {
	return func(handler providers.CallContextFunc) providers.CallContextFunc {
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
}

func middlewareLog(fullnode, space string) providers.CallContextMiddleware {
	return func(handler providers.CallContextFunc) providers.CallContextFunc {
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
}
