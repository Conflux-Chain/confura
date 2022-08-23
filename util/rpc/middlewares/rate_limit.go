package middlewares

import (
	"context"
	"errors"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	web3pay "github.com/Conflux-Chain/web3pay-service/client"
	lru "github.com/hashicorp/golang-lru"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

var (
	errRateLimit           = errors.New("too many requests")
	vipAccessTokenCache, _ = lru.New(2000)
)

func RateLimitBatch(next rpc.HandleBatchFunc) rpc.HandleBatchFunc {
	return func(ctx context.Context, msgs []*rpc.JsonRpcMessage) []*rpc.JsonRpcMessage {
		if handlers.WhiteListAllow(ctx) || handlers.RateLimitAllow(ctx, "rpc_batch", len(msgs)) {
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
		// check billing status
		if bs, ok := web3pay.BillingStatusFromContext(ctx); ok {
			token, _ := handlers.GetAccessTokenFromContext(ctx)

			// serve on billing successfully
			if bs.Success() {
				vipAccessTokenCache.Add(token, struct{}{})
				return next(ctx, msg)
			}

			// handle internal server errors
			if err, ok := bs.InternalServerError(); ok {
				logrus.WithField("msg", msg).WithError(err).Error("Billing internal server error")

				// try the best not to block VIP users due to internal server errors
				if _, ok := vipAccessTokenCache.Get(token); ok {
					return next(ctx, msg)
				}
			}

			// otherwise fallback to rate limit
		}

		// white list allow?
		if handlers.WhiteListAllow(ctx) {
			return next(ctx, msg)
		}

		// overall rate limit
		if !handlers.RateLimitAllow(ctx, "rpc_all", 1) {
			return msg.ErrorResponse(errRateLimit)
		}

		// single method rate limit
		if !handlers.RateLimitAllow(ctx, msg.Method, 1) {
			return msg.ErrorResponse(errRateLimit)
		}

		return next(ctx, msg)
	}
}
