package middlewares

import (
	"context"

	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

func Auth() rpc.HandleCallMsgMiddleware {
	// web3pay
	if mw, conf, ok := MustNewWeb3PayMiddlewareFromViper(); ok {
		logrus.WithField("mode", conf.Mode).Info("Web3Pay openweb3 RPC middleware enabled")

		return func(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
			return mw(Authenticate(next))
		}
	}

	return Authenticate
}

func Authenticate(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		if !handlers.IsAccessTokenValid(ctx) {
			return next(ctx, msg)
		}

		if vs, ok := handlers.VipStatusFromContext(ctx); ok { // access from web3pay VIP user
			ctx = context.WithValue(ctx, handlers.CtxKeyAuthId, vs.ID)
		} else if svs, ok := rate.SVipStatusFromContext(ctx); ok { // access from SVIP user
			ctx = context.WithValue(ctx, handlers.CtxKeyAuthId, svs.Key)
		}

		return next(ctx, msg)
	}
}
