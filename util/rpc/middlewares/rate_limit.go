package middlewares

import (
	"context"
	"errors"

	"github.com/conflux-chain/conflux-infura/util/rate"
	"github.com/openweb3/go-rpc-provider"
)

var errRateLimit = errors.New("too many requests")

func RateLimit(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		if rate.Allow(ctx, msg.Method, 1) {
			return next(ctx, msg)
		}

		return msg.ErrorResponse(errRateLimit)
	}
}
