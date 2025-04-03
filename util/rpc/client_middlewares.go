package rpc

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/confura/util/rpc/cache"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	"github.com/openweb3/go-rpc-provider/utils"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type cacheHandlerCtxKey struct{}

// cacheHandlerFunc defines the cache handler functions that can retrieve or load cached results for a given RPC method call.
type cacheHandlerFunc func(
	ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error)

var (
	// cacheHandlers maps RPC method names to their associated cache handler functions.
	cacheHandlers map[string]cacheHandlerFunc // method name => cache handler function

	ctxKeyToHeaderKeys = map[handlers.CtxKey]string{
		handlers.CtxKeyAccessToken: "Access-Token",
		handlers.CtxKeyReqOrigin:   "Origin",
		handlers.CtxKeyUserAgent:   "User-Agent",
		handlers.CtxKeyRealIP:      "X-Real-Ip",
	}
)

func init() {
	cacheHandlers = map[string]cacheHandlerFunc{
		// Core space cache handlers
		"cfx_clientVersion": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			return cache.CfxDefault.GetClientVersionWithFunc(upstreamHandler)
		},
		"cfx_gasPrice": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			return cache.CfxDefault.GetGasPriceWithFunc(upstreamHandler)
		},
		"cfx_getStatus": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			nodeName := ctx.Value(cacheHandlerCtxKey{}).(string)
			return cache.CfxDefault.GetStatusWithFunc(nodeName, upstreamHandler)
		},
		"cfx_epochNumber": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			epoch, err := parseCfxEpochNumberArgument(args...)
			if err != nil {
				return nil, false, err
			}
			nodeName := ctx.Value(cacheHandlerCtxKey{}).(string)
			return cache.CfxDefault.GetEpochNumberWithFunc(nodeName, upstreamHandler, epoch)
		},
		"cfx_getBestBlockHash": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			nodeName := ctx.Value(cacheHandlerCtxKey{}).(string)
			return cache.CfxDefault.GetBestBlockHashWithFunc(nodeName, upstreamHandler)
		},
		// EVM space cache handlers
		"eth_chainId": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			return cache.EthDefault.GetChainIdWithFunc(upstreamHandler)
		},
		"eth_gasPrice": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			return cache.EthDefault.GetGasPriceWithFunc(upstreamHandler)
		},
		"eth_blockNumber": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			nodeName := ctx.Value(cacheHandlerCtxKey{}).(string)
			return cache.EthDefault.GetBlockNumberWithFunc(nodeName, upstreamHandler)
		},
		"eth_getBlockByNumber": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			blockNum, includeTxs, err := parseEthBlockByNumberArguments(args...)
			if err != nil {
				return nil, false, err
			}
			nodeName := ctx.Value(cacheHandlerCtxKey{}).(string)
			return cache.EthDefault.GetBlockByNumberWithFunc(nodeName, upstreamHandler, blockNum, includeTxs)
		},
		"eth_call": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			request, blockNumOrHash, err := parseEthCallArguments(args...)
			if err != nil {
				return nil, false, err
			}
			nodeName := ctx.Value(cacheHandlerCtxKey{}).(string)
			return cache.EthDefault.CallWithFunc(nodeName, upstreamHandler, request, blockNumOrHash)
		},
		"net_version": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			return cache.EthDefault.GetNetVersionWithFunc(upstreamHandler)
		},
		"web3_clientVersion": func(ctx context.Context, upstreamHandler func() (interface{}, error), args ...interface{}) (interface{}, bool, error) {
			return cache.EthDefault.GetClientVersionWithFunc(upstreamHandler)
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

	if flag&MiddlewareHookCache != 0 {
		provider.HookCallContext(middlewareCache(nodeName, space))
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

// middlewareCache returns a middleware that attempts to fetch cached responses for certain RPC methods before invoking
// the upstream handler. If no cached result is available for the requested method, it falls back to the upstream handler.
func middlewareCache(fullnode, space string) providers.CallContextMiddleware {
	return func(handler providers.CallContextFunc) providers.CallContextFunc {
		return func(ctx context.Context, result interface{}, method string, args ...interface{}) error {
			cacheHandler, ok := cacheHandlers[method]
			if !ok {
				return handler(ctx, result, method, args...)
			}

			ctx = context.WithValue(ctx, cacheHandlerCtxKey{}, fullnode)
			val, loaded, err := cacheHandler(ctx, func() (interface{}, error) {
				err := handler(ctx, result, method, args...)
				if err != nil {
					return nil, err
				}
				// Extract the underlying value from the result
				return reflect.ValueOf(result).Elem().Interface(), nil
			}, args...)
			if err != nil {
				return err
			}

			if logrus.IsLevelEnabled(logrus.DebugLevel) {
				logrus.WithFields(logrus.Fields{
					"fullnode": fullnode,
					"space":    space,
					"method":   method,
					"args":     args,
					"cacheHit": loaded,
				}).Debug("RPC method cache loaded")
			}
			metrics.Registry.Client.CacheHit(method).Mark(loaded)
			return processCacheResult(result, val)
		}
	}
}

func parseCfxEpochNumberArgument(args ...interface{}) (*types.Epoch, error) {
	if len(args) == 0 {
		return nil, nil
	}
	val, ok := args[0].(*types.Epoch)
	if !ok {
		return nil, errors.Errorf(
			"invalid argument: expected type `*types.Epoch`, but received %T", args[0],
		)
	}
	return val, nil
}

func parseEthCallArguments(args ...interface{}) (web3Types.CallRequest, *web3Types.BlockNumberOrHash, error) {
	if len(args) == 0 {
		return web3Types.CallRequest{}, nil, errors.New("invalid argument: expected at least one argument")
	}

	var request web3Types.CallRequest
	var blockNumOrHash *web3Types.BlockNumberOrHash

	// Validate and extract the first argument
	request, ok := args[0].(web3Types.CallRequest)
	if !ok {
		return request, nil, errors.Errorf(
			"invalid argument: the first argument must be of type `web3Types.CallRequest`, but received %T", args[0],
		)
	}
	// Validate and extract the second argument
	if len(args) > 1 {
		blockNumOrHash, ok = args[1].(*web3Types.BlockNumberOrHash)
		if !ok {
			return request, nil, errors.Errorf(
				"invalid argument: the second argument must be of type `*web3Types.BlockNumberOrHash`, but received %T", args[1],
			)
		}
	}
	return request, blockNumOrHash, nil
}

func parseEthBlockByNumberArguments(args ...interface{}) (blockNum web3Types.BlockNumber, includeTxs bool, err error) {
	if len(args) != 2 {
		return blockNum, false, errors.New("invalid argument: expected two arguments")
	}

	// Validate and extract the first argument
	blockNum, ok := args[0].(web3Types.BlockNumber)
	if !ok {
		return blockNum, false, errors.Errorf(
			"invalid argument: the first argument must be of type `web3Types.BlockNumber`, but received %T", args[0],
		)
	}
	// Validate and extract the second argument
	includeTxs, ok = args[1].(bool)
	if !ok {
		return blockNum, false, errors.Errorf(
			"invalid argument: the second argument must be of type `bool`, but received %T", args[1],
		)
	}

	return blockNum, includeTxs, nil
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

func processCacheResult(result interface{}, val interface{}) error {
	if rpcResult, isRPCResult := val.(cache.RPCResult); isRPCResult {
		if rpcResult.RpcError != nil {
			return rpcResult.RpcError
		}
		return setResult(result, rpcResult.Data)
	}

	return setResult(result, val)
}
