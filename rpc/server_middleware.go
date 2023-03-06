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
	"github.com/sirupsen/logrus"
)

const (
	ctxKeyClientProvider = handlers.CtxKey("Infura-RPC-Client-Provider")
	ctxKeyClientSet      = handlers.CtxKey("Infura-RPC-Client-Set")
)

// go-rpc-provider only supports static middlewares for RPC server.
func init() {
	// middlewares executed in order

	// panic recovery
	rpc.HookHandleCallMsg(middlewares.Recover)

	// anti-injection
	rpc.HookHandleCallMsg(middlewares.AntiInjection)

	// web3pay
	if mw, conf, ok := middlewares.MustNewWeb3PayMiddlewareFromViper(); ok {
		logrus.WithField("mode", conf.Mode).Info("Web3Pay openweb3 RPC middleware enabled")
		rpc.HookHandleCallMsg(mw)
	}

	// VIP only access control
	acmw := middlewares.MustNewVipOnlyAccessControlMiddlewareFromViper()
	rpc.HookHandleCallMsg(acmw)

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
}

// Inject values into context for static RPC call middlewares, e.g. rate limit
func httpMiddleware(registry *rate.Registry, clientProvider interface{}) handlers.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			if token := handlers.GetAccessToken(r); len(token) > 0 { // optional
				ctx = context.WithValue(ctx, handlers.CtxAccessToken, token)
			}

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
		var clients interface{}
		var err error

		if cfxProvider, ok := ctx.Value(ctxKeyClientProvider).(*node.CfxClientProvider); ok {
			switch msg.Method {
			case "cfx_getLogs":
				clients, err = getCfxClientSetFromProviderWithContext(ctx, cfxProvider, node.GroupCfxLogs)
			default:
				clients, err = getCfxClientSetFromProviderWithContext(ctx, cfxProvider)
			}
		} else if ethProvider, ok := ctx.Value(ctxKeyClientProvider).(*node.EthClientProvider); ok {
			switch msg.Method {
			case "eth_getLogs":
				clients, err = getEthClientSetFromProviderWithContext(ctx, ethProvider, node.GroupEthLogs)
			default:
				clients, err = getEthClientSetFromProviderWithContext(ctx, ethProvider)
			}
		} else {
			return next(ctx, msg)
		}

		// no fullnode available to request RPC
		if err != nil {
			return msg.ErrorResponse(err)
		}

		ctx = context.WithValue(ctx, ctxKeyClientSet, clients)
		return next(ctx, msg)
	}
}

func GetCfxClientSetFromContext(ctx context.Context) []sdk.ClientOperator {
	return ctx.Value(ctxKeyClientSet).([]sdk.ClientOperator)
}

func GetEthClientSetFromContext(ctx context.Context) []*node.Web3goClient {
	return ctx.Value(ctxKeyClientSet).([]*node.Web3goClient)
}

func GetCfxClientFromContext(ctx context.Context) sdk.ClientOperator {
	return GetCfxClientSetFromContext(ctx)[0]
}

func GetEthClientFromContext(ctx context.Context) *node.Web3goClient {
	return GetEthClientSetFromContext(ctx)[0]
}

func getEthClientSetFromProviderWithContext(
	ctx context.Context,
	p *node.EthClientProvider,
	group ...node.Group,
) ([]*node.Web3goClient, error) {
	if _, ok := handlers.VipStatusFromContext(ctx); ok {
		return p.GetClientSetByToken(ctx, group...)
	}

	if _, ok := rate.SVipStatusFromContext(ctx); ok {
		return p.GetClientSetByToken(ctx, group...)
	}

	return p.GetClientSetByIP(ctx, group...)
}

func getCfxClientSetFromProviderWithContext(
	ctx context.Context,
	p *node.CfxClientProvider,
	group ...node.Group,
) ([]sdk.ClientOperator, error) {
	if _, ok := handlers.VipStatusFromContext(ctx); ok {
		return p.GetClientSetByToken(ctx, group...)
	}

	if _, ok := rate.SVipStatusFromContext(ctx); ok {
		return p.GetClientSetByToken(ctx, group...)
	}

	return p.GetClientSetByIP(ctx, group...)
}
