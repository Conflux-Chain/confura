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
	ctxKeyClient         = handlers.CtxKey("Infura-RPC-Client")
	ctxKeyClientGroup    = handlers.CtxKey("Infura-RPC-Client-Group")
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
		var client interface{}
		var grp node.Group
		var err error

		if cfxProvider, ok := ctx.Value(ctxKeyClientProvider).(*node.CfxClientProvider); ok {
			switch msg.Method {
			case "cfx_getLogs":
				client, grp, err = getCfxClientFromProviderWithContext(ctx, cfxProvider, node.GroupCfxLogs)
			default:
				client, grp, err = getCfxClientFromProviderWithContext(ctx, cfxProvider, node.GroupCfxHttp)
			}
		} else if ethProvider, ok := ctx.Value(ctxKeyClientProvider).(*node.EthClientProvider); ok {
			switch msg.Method {
			case "eth_getLogs":
				client, grp, err = getEthClientFromProviderWithContext(ctx, ethProvider, node.GroupEthLogs)
			default:
				client, grp, err = getEthClientFromProviderWithContext(ctx, ethProvider, node.GroupEthHttp)
			}
		} else {
			return next(ctx, msg)
		}

		// no fullnode available to request RPC
		if err != nil {
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
	ctx context.Context,
	p *node.EthClientProvider,
	fallback node.Group,
) (*node.Web3goClient, node.Group, error) {
	if isVipAccessFromContext(ctx) { // vip access
		grp, ok := p.GetGroupByToken(ctx)
		if ok && len(grp) > 0 {
			client, err := p.GetClientByToken(ctx, grp)
			return client, grp, err
		}
	}

	client, err := p.GetClientByIP(ctx, fallback)
	return client, fallback, err
}

func getCfxClientFromProviderWithContext(
	ctx context.Context,
	p *node.CfxClientProvider,
	fallback node.Group,
) (sdk.ClientOperator, node.Group, error) {
	if isVipAccessFromContext(ctx) { // vip access
		grp, ok := p.GetGroupByToken(ctx)
		if ok && len(grp) > 0 {
			client, err := p.GetClientByToken(ctx, grp)
			return client, grp, err
		}
	}

	client, err := p.GetClientByIP(ctx, fallback)
	return client, fallback, err
}

func isVipAccessFromContext(ctx context.Context) bool {
	if _, ok := handlers.VipStatusFromContext(ctx); ok {
		return true
	}

	if _, ok := rate.SVipStatusFromContext(ctx); ok {
		return true
	}

	return false
}
