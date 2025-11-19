package middlewares

import (
	"context"

	"github.com/Conflux-Chain/confura/util/acl"
	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
)

func Allowlists(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		registry, ok := ctx.Value(handlers.CtxKeyRateRegistry).(*rate.Registry)
		if !ok {
			return next(ctx, msg)
		}

		aclCtx := acl.Context{
			Context:   ctx,
			RpcMethod: msg.Method,
		}

		if connH, ok := ctx.Value("handler").(*rpc.ConnHandler); ok {
			aclCtx.ExtractRpcParams = func() ([]any, error) {
				return connH.ParseCallArguments(msg)
			}
		}

		if err := registry.Allow(aclCtx); err != nil {
			return msg.ErrorResponse(errAllowlistsForbidden(err))
		}

		return next(ctx, msg)
	}
}

func errAllowlistsForbidden(err error) error {
	return errors.WithMessage(err, "access forbidden by allowlists")
}
