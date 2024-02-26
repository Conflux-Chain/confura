package rpc

import (
	"context"
	"net/http"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/confura/util/rpc/middlewares"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/openweb3/go-rpc-provider"
)

const (
	ctxKeyClientProvider = handlers.CtxKey("Infura-RPC-Client-Provider")
	ctxKeyClient         = handlers.CtxKey("Infura-RPC-Client")
	ctxKeyClientGroup    = handlers.CtxKey("Infura-RPC-Client-Group")
)

func MustInit() {
	// init metrics
	initMetrics()

	// Register middlewares for go-rpc-provider, which only supports static middlewares for RPC server.
	// The following middlewares are executed in order.

	// panic recovery
	rpc.HookHandleCallMsg(middlewares.Recover)

	// anti-injection
	rpc.HookHandleCallMsg(middlewares.AntiInjection)

	// auth
	rpc.HookHandleCallMsg(middlewares.Auth())

	// allow lists
	rpc.HookHandleCallMsg(middlewares.Allowlists)

	// rate limit
	rpc.HookHandleCallMsg(middlewares.DailyMaxReqRateLimit)
	rpc.HookHandleCallMsg(middlewares.QpsRateLimit)

	// metrics
	rpc.HookHandleBatch(middlewares.MetricsBatch)
	rpc.HookHandleCallMsg(middlewares.Metrics)

	// log
	rpc.HookHandleBatch(middlewares.LogBatch)
	rpc.HookHandleCallMsg(middlewares.Log)

	// cfx/eth client
	rpc.HookHandleCallMsg(clientMiddleware)

	// invalid json rpc request without `ID`
	rpc.HookHandleCallMsg(rpc.PreventMessagesWithouID)

	// uniform human-readable error message
	rpc.HookHandleCallMsg(middlewares.UniformError)
}

// Inject values into context for static RPC call middlewares, e.g. rate limit
func httpMiddleware(registry *rate.Registry, clientProvider interface{}) handlers.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			if token := handlers.GetAccessToken(r); len(token) > 0 { // optional
				ctx = context.WithValue(ctx, handlers.CtxKeyAccessToken, token)
			}

			ctx = context.WithValue(ctx, handlers.CtxKeyReqOrigin, r.Header.Get("Origin"))
			ctx = context.WithValue(ctx, handlers.CtxKeyUserAgent, r.Header.Get("User-Agent"))
			ctx = context.WithValue(ctx, handlers.CtxKeyRealIP, handlers.GetIPAddress(r))

			if registry != nil {
				ctx = context.WithValue(ctx, handlers.CtxKeyRateRegistry, registry)
			}

			if clientProvider != nil {
				ctx = context.WithValue(ctx, ctxKeyClientProvider, clientProvider)
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func clientMiddleware(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		var client interface{}
		var grp node.Group
		var err error

		if cfxProvider, ok := ctx.Value(ctxKeyClientProvider).(*node.CfxClientProvider); ok {
			client, grp, err = getCfxClientFromProviderWithContext(ctx, msg.Method, cfxProvider)
		} else if ethProvider, ok := ctx.Value(ctxKeyClientProvider).(*node.EthClientProvider); ok {
			client, grp, err = getEthClientFromProviderWithContext(ctx, msg.Method, ethProvider)
		} else {
			return next(ctx, msg)
		}

		if err != nil { // no fullnode available to request RPC
			return msg.ErrorResponse(err)
		}

		ctx = context.WithValue(ctx, ctxKeyClient, client)
		ctx = context.WithValue(ctx, ctxKeyClientGroup, grp)

		return next(ctx, msg)
	}
}

func GetCfxClientFromContext(ctx context.Context) sdk.ClientOperator {
	return ctx.Value(ctxKeyClient).(sdk.ClientOperator)
}

func GetEthClientFromContext(ctx context.Context) *node.Web3goClient {
	return ctx.Value(ctxKeyClient).(*node.Web3goClient)
}

func GetClientGroupFromContext(ctx context.Context) node.Group {
	return ctx.Value(ctxKeyClientGroup).(node.Group)
}

func getEthClientFromProviderWithContext(
	ctx context.Context, rpcMethod string, p *node.EthClientProvider) (*node.Web3goClient, node.Group, error) {
	grp := node.GroupEthHttp

	switch {
	case rpcMethod == rpcMethodEthGetLogs:
		grp = node.GroupEthLogs
	case isEthFilterRpcMethod(rpcMethod):
		grp = node.GroupEthFilter
	default:
		if authId, ok := handlers.GetAuthIdFromContext(ctx); ok {
			grp, ok := p.GetRouteGroup(authId)
			if ok && len(grp) > 0 {
				client, err := p.GetClient(authId, grp)
				return client, grp, err
			}
		}
	}

	client, err := p.GetClientByIP(ctx, grp)
	return client, grp, err
}

func getCfxClientFromProviderWithContext(
	ctx context.Context, rpcMethod string, p *node.CfxClientProvider) (sdk.ClientOperator, node.Group, error) {
	grp := node.GroupCfxHttp

	switch {
	case rpcMethod == rpcMethodCfxGetLogs:
		grp = node.GroupCfxLogs
	case isCfxFilterRpcMethod(rpcMethod):
		grp = node.GroupCfxFilter
	default:
		if authId, ok := handlers.GetAuthIdFromContext(ctx); ok {
			grp, ok := p.GetRouteGroup(authId)
			if ok && len(grp) > 0 {
				client, err := p.GetClient(authId, grp)
				return client, grp, err
			}
		}
	}

	client, err := p.GetClientByIP(ctx, grp)
	return client, grp, err
}
