package rpc

import (
	"context"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/openweb3/go-rpc-provider"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var ctxKeyToHeaderKeys = map[handlers.CtxKey]string{
	handlers.CtxKeyAccessToken: "Access-Token",
	handlers.CtxKeyReqOrigin:   "Origin",
	handlers.CtxKeyUserAgent:   "User-Agent",
	handlers.CtxKeyRealIP:      "X-Real-Ip",
}

// HookRedirectHttpHeader registers an event handler before sending client HTTP request,
// which will forward HTTP headers to the requested RPC service.
func HookRedirectHttpHeader() {
	rpc.RegisterBeforeSendHttp(func(ctx context.Context, req *fasthttp.Request) error {
		for ctxKey, header := range ctxKeyToHeaderKeys {
			if val, ok := ctx.Value(ctxKey).(string); ok {
				req.Header.Set(header, val)
			}
		}

		return nil
	})
}

func Url2NodeName(url string) string {
	nodeName := strings.ToLower(url)
	nodeName = strings.TrimPrefix(nodeName, "http://")
	nodeName = strings.TrimPrefix(nodeName, "https://")
	nodeName = strings.TrimPrefix(nodeName, "ws://")
	nodeName = strings.TrimPrefix(nodeName, "wss://")
	return strings.TrimPrefix(nodeName, "/")
}

// MiddlewareHookFlag represents the type for middleware hook flags.
type MiddlewareHookFlag int

const (
	// MiddlewareHookNone represents no middleware hooks enabled.
	MiddlewareHookNone MiddlewareHookFlag = 0

	// MiddlewareHookAll enables all middleware hooks.
	MiddlewareHookAll MiddlewareHookFlag = ^MiddlewareHookFlag(0)

	// MiddlewareHookLog enables logging middleware hook.
	MiddlewareHookLog MiddlewareHookFlag = 1 << iota

	// MiddlewareHookLogMetrics enables metrics logging middleware hook.
	MiddlewareHookLogMetrics
)

func HookMiddlewares(provider *providers.MiddlewarableProvider, url, space string, flags ...MiddlewareHookFlag) {
	nodeName := Url2NodeName(url)

	flag := MiddlewareHookAll
	if len(flags) > 0 {
		flag = flags[0]
	}

	if flag&MiddlewareHookLog != 0 {
		provider.HookCallContext(middlewareLog(nodeName, space))
	}

	if flag&MiddlewareHookLogMetrics != 0 {
		provider.HookCallContext(middlewareMetrics(nodeName, space))
	}
}

func middlewareMetrics(fullnode, space string) providers.CallContextMiddleware {
	return func(handler providers.CallContextFunc) providers.CallContextFunc {
		return func(ctx context.Context, result interface{}, method string, args ...interface{}) error {
			start := time.Now()

			err := handler(ctx, result, method, args...)

			metrics.Registry.RPC.FullnodeQps(fullnode, space, method, err).UpdateSince(start)

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
			} else if logrus.IsLevelEnabled(logrus.TraceLevel) {
				logger = logger.WithField("result", result)
			}

			logger.Debug("RPC leave")

			return err
		}
	}
}
