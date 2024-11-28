package rpc

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/confura/util/rpc/cache"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	"github.com/openweb3/go-rpc-provider/utils"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type CacheHandlerFunc[T any] func(client T, nodeName string, args ...interface{}) (interface{}, error)

var (
	cacheFnClients   util.ConcurrentMap
	cfxCacheHandlers map[string]CacheHandlerFunc[sdk.ClientOperator] // method => cache handler
	ethCacheHandlers map[string]CacheHandlerFunc[*web3go.Client]     // method => cache handler

	ctxKeyToHeaderKeys = map[handlers.CtxKey]string{
		handlers.CtxKeyAccessToken: "Access-Token",
		handlers.CtxKeyReqOrigin:   "Origin",
		handlers.CtxKeyUserAgent:   "User-Agent",
		handlers.CtxKeyRealIP:      "X-Real-Ip",
	}
)

func init() {
	cfxCacheHandlers = map[string]CacheHandlerFunc[sdk.ClientOperator]{
		"cfx_clientVersion": func(cfx sdk.ClientOperator, nodeName string, args ...interface{}) (interface{}, error) {
			return cache.CfxDefault.GetClientVersion(cfx)
		},
		"cfx_gasPrice": func(cfx sdk.ClientOperator, nodeName string, args ...interface{}) (interface{}, error) {
			return cache.CfxDefault.GetGasPrice(cfx)
		},
		"cfx_getStatus": func(cfx sdk.ClientOperator, nodeName string, args ...interface{}) (interface{}, error) {
			return cache.CfxDefault.GetStatus(nodeName, cfx)
		},
		"cfx_epochNumber": func(cfx sdk.ClientOperator, nodeName string, args ...interface{}) (interface{}, error) {
			var epoch *types.Epoch
			if len(args) > 0 {
				epoch = args[0].(*types.Epoch)
			}
			return cache.CfxDefault.GetEpochNumber(nodeName, cfx, epoch)
		},
		"cfx_getBestBlockHash": func(cfx sdk.ClientOperator, nodeName string, args ...interface{}) (interface{}, error) {
			return cache.CfxDefault.GetBestBlockHash(nodeName, cfx)
		},
	}
	ethCacheHandlers = map[string]CacheHandlerFunc[*web3go.Client]{
		"eth_chainId": func(w3c *web3go.Client, nodeName string, args ...interface{}) (interface{}, error) {
			return cache.EthDefault.GetChainId(w3c)
		},
		"eth_gasPrice": func(w3c *web3go.Client, nodeName string, args ...interface{}) (interface{}, error) {
			return cache.EthDefault.GetGasPrice(w3c)
		},
		"eth_blockNumber": func(w3c *web3go.Client, nodeName string, args ...interface{}) (interface{}, error) {
			return cache.EthDefault.GetBlockNumber(nodeName, w3c)
		},
		"net_version": func(w3c *web3go.Client, nodeName string, args ...interface{}) (interface{}, error) {
			return cache.EthDefault.GetNetVersion(w3c)
		},
		"web3_clientVersion": func(w3c *web3go.Client, nodeName string, args ...interface{}) (interface{}, error) {
			return cache.EthDefault.GetClientVersion(w3c)
		},
	}
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

	// MiddlewareHookCache enables cache middleware hook.
	MiddlewareHookCache
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

	if flag&MiddlewareHookCache != 0 {
		provider.HookCallContext(middlewareCache(url, space))
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

func middlewareCache(url, space string) providers.CallContextMiddleware {
	switch space {
	case "eth":
		return ethCacheMiddleware(url)
	case "cfx":
		return cfxCacheMiddleware(url)
	default:
		panic(fmt.Sprintf("unsupported space: %v", space))
	}
}

func cfxCacheMiddleware(url string) providers.CallContextMiddleware {
	nodeName := Url2NodeName(url)
	return func(handler providers.CallContextFunc) providers.CallContextFunc {
		return func(ctx context.Context, result interface{}, method string, args ...interface{}) error {
			cacheHandler, ok := cfxCacheHandlers[method]
			if !ok {
				return handler(ctx, result, method, args...)
			}
			cfx, err := getCfxClient(url)
			if err != nil {
				return err
			}
			val, err := cacheHandler(cfx, nodeName, args...)
			if err != nil {
				return err
			}
			return setResult(result, val)
		}
	}
}

func ethCacheMiddleware(url string) providers.CallContextMiddleware {
	nodeName := Url2NodeName(url)
	return func(handler providers.CallContextFunc) providers.CallContextFunc {
		return func(ctx context.Context, result interface{}, method string, args ...interface{}) error {
			cacheHandler, ok := ethCacheHandlers[method]
			if !ok {
				return handler(ctx, result, method, args...)
			}
			eth, err := getEthClient(url)
			if err != nil {
				return err
			}
			val, err := cacheHandler(eth, nodeName, args...)
			if err != nil {
				return err
			}
			return setResult(result, val)
		}
	}
}

func getCfxClient(url string) (sdk.ClientOperator, error) {
	val, _, err := cacheFnClients.LoadOrStoreFnErr(url, func(k interface{}) (interface{}, error) {
		return NewCfxClient(url)
	})
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create cfx client")
	}

	return val.(sdk.ClientOperator), nil
}

func getEthClient(url string) (*web3go.Client, error) {
	val, _, err := cacheFnClients.LoadOrStoreFnErr(url, func(k interface{}) (interface{}, error) {
		return NewEthClient(url)
	})
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create eth client")
	}

	return val.(*web3go.Client), nil
}

func setResult(result interface{}, val interface{}) error {
	// Ensure result is a non-nil pointer
	resultValue := reflect.ValueOf(result)
	if resultValue.Kind() != reflect.Ptr || resultValue.IsNil() {
		return fmt.Errorf("result must be a non-nil pointer, got: %T", result)
	}

	// Dereference the pointer
	resultElem := resultValue.Elem()

	// Check if the value can be assigned
	value := reflect.ValueOf(val)
	if !value.Type().AssignableTo(resultElem.Type()) {
		return fmt.Errorf("type mismatch: cannot assign %v to %v", value.Type(), resultElem.Type())
	}

	// Set the value
	resultElem.Set(value)
	return nil
}
