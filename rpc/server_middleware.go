package rpc

import (
	"context"
	"net/http"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/openweb3/go-rpc-provider"
	"github.com/scroll-tech/rpc-gateway/node"
	"github.com/scroll-tech/rpc-gateway/util/rate"
	"github.com/scroll-tech/rpc-gateway/util/rpc/handlers"
	"github.com/scroll-tech/rpc-gateway/util/rpc/middlewares"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	ctxKeyClientProvider = handlers.CtxKey("Infura-RPC-Client-Provider")
	ctxKeyClient         = handlers.CtxKey("Infura-RPC-Client")
)

// go-rpc-provider only supports static middlewares for RPC server.
func init() {
	viper.SetDefault("rpc.loadBalancerMode", "consistentHashing")
	// middlewares executed in order

	// panic recovery
	rpc.HookHandleCallMsg(middlewares.Recover)

	// web3pay billing
	if web3payClient, ok := middlewares.MustNewWeb3PayClient(); ok {
		logrus.Info("Web3Pay billing RPC middleware enabled")
		rpc.HookHandleCallMsg(middlewares.Billing(web3payClient))
	}

	// rate limit
	rpc.HookHandleBatch(middlewares.RateLimitBatch)
	rpc.HookHandleCallMsg(middlewares.RateLimit)

	// metrics
	rpc.HookHandleBatch(middlewares.MetricsBatch)
	rpc.HookHandleCallMsg(middlewares.Metrics)

	// log
	rpc.HookHandleBatch(middlewares.LogBatch)
	rpc.HookHandleCallMsg(middlewares.Log)

	// cfx/eth client
	rpc.HookHandleCallMsg(clientMiddleware)

	// invalid json rpc request without `ID``
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
			ctx = context.WithValue(ctx, handlers.CtxKeyRateRegistry, registry)
			ctx = context.WithValue(ctx, ctxKeyClientProvider, clientProvider)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func clientMiddleware(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	loadBalancerMode := viper.GetString("rpc.loadBalancerMode")
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		var client interface{}
		var err error

		if cfxProvider, ok := ctx.Value(ctxKeyClientProvider).(*node.CfxClientProvider); ok {
			switch msg.Method {
			case "cfx_getLogs":
				client, err = cfxProvider.GetClientByIPGroup(ctx, node.GroupCfxLogs)
			default:
				client, err = cfxProvider.GetClientByIP(ctx)
			}
		} else if ethProvider, ok := ctx.Value(ctxKeyClientProvider).(*node.EthClientProvider); ok {
			if (loadBalancerMode == "consistentHashing") {
				switch msg.Method {
				case "eth_getLogs":
					client, err = ethProvider.GetClientByIPGroup(ctx, node.GroupEthLogs)
				default:
					client, err = ethProvider.GetClientByIP(ctx)
				}
			} else {
				switch msg.Method{
				case "eth_getLogs":
					client, err = ethProvider.GetClientRandomByGroup(node.GroupEthLogs)
				default:
					client, err = ethProvider.GetClientRandom()
				}
			}
		} else {
			return next(ctx, msg)
		}

		// no fullnode available to request RPC
		if err != nil {
			return msg.ErrorResponse(err)
		}

		ctx = context.WithValue(ctx, ctxKeyClient, client)

		return next(ctx, msg)
	}
}

func GetCfxClientFromContext(ctx context.Context) sdk.ClientOperator {
	return ctx.Value(ctxKeyClient).(sdk.ClientOperator)
}

func GetEthClientFromContext(ctx context.Context) *node.Web3goClient {
	return ctx.Value(ctxKeyClient).(*node.Web3goClient)
}
