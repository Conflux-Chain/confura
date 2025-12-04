package middlewares

import (
	"context"
	"fmt"

	"github.com/Conflux-Chain/confura/util/rate"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
)

const (
	ratelimitErrorCode = -32005
)

func QpsRateLimit(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		registry, ok := ctx.Value(handlers.CtxKeyRateRegistry).(*rate.Registry)
		if !ok {
			return next(ctx, msg)
		}

		// overall rate limit
		if err := registry.Limit(ctx, "rpc_all_qps"); err != nil {
			return msg.ErrorResponse(errQpsRateLimited(err))
		}

		// single method rate limit
		resource := fmt.Sprintf("%v_qps", msg.Method)
		if err := registry.Limit(ctx, resource); err != nil {
			return msg.ErrorResponse(errQpsRateLimited(err))
		}

		return next(ctx, msg)
	}
}

func errQpsRateLimited(err error) error {
	return &rpc.JsonError{
		Code:    ratelimitErrorCode,
		Message: errors.WithMessage(err, "request rate exceeded").Error() + errDocHint,
	}
}

func DailyMaxReqRateLimit(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		registry, ok := ctx.Value(handlers.CtxKeyRateRegistry).(*rate.Registry)
		if !ok {
			return next(ctx, msg)
		}

		// constrain daily total requests
		if err := registry.Limit(ctx, "rpc_all_daily"); err != nil {
			return msg.ErrorResponse(errDailyMaxReqRateLimited(err))
		}

		return next(ctx, msg)
	}
}

func errDailyMaxReqRateLimited(err error) error {
	return &rpc.JsonError{
		Code:    ratelimitErrorCode,
		Message: errors.WithMessage(err, "daily request count exceeded").Error() + errDocHint,
	}
}
