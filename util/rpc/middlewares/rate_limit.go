package middlewares

import (
	"context"
	"errors"

	"github.com/conflux-chain/conflux-infura/util/rate"
	"github.com/openweb3/go-rpc-provider"
)

var errRateLimit = errors.New("too many requests")

func RateLimitBatch(next rpc.HandleBatchFunc) rpc.HandleBatchFunc {
	return func(ctx context.Context, msgs []*rpc.JsonRpcMessage) []*rpc.JsonRpcMessage {
		if rate.Allow(ctx, "rpc_all", len(msgs)) {
			return next(ctx, msgs)
		}

		var responses []*rpc.JsonRpcMessage
		for _, v := range msgs {
			responses = append(responses, v.ErrorResponse(errRateLimit))
		}

		return responses
	}
}

func RateLimit(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		if rate.Allow(ctx, msg.Method, 1) {
			return next(ctx, msg)
		}

		return msg.ErrorResponse(errRateLimit)
	}
}
