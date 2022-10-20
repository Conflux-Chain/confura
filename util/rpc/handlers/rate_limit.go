package handlers

import (
	"context"
	"net/http"

	"github.com/Conflux-Chain/confura/util/rate"
	web3pay "github.com/Conflux-Chain/web3pay-service/client/middleware"
)

func RateLimit(registry *rate.Registry) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), CtxKeyRateRegistry, registry)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func RateLimitAllow(ctx context.Context, name string, n int) bool {
	registry, ok := ctx.Value(CtxKeyRateRegistry).(*rate.Registry)
	if !ok {
		return true
	}

	ip, ok := GetIPAddressFromContext(ctx)
	if !ok { // ip is mandatory
		return true
	}

	// access token is optional
	token, _ := GetAccessTokenFromContext(ctx)

	vc := &rate.VisitContext{
		Ip: ip, Resource: name, Key: token,
	}

	if ss, ok := web3pay.VipSubscriptionStatusFromContext(ctx); ok {
		// check VIP subscription status
		vc.Status, _ = rate.GetVipStatusBySubscriptionStatus(ss)
	} else if bs, ok := web3pay.BillingStatusFromContext(ctx); ok {
		// check billing status
		vc.Status, _ = rate.GetVipStatusByBillingStatus(bs)
	}

	limiter, ok := registry.Get(vc)
	if !ok {
		return true
	}

	return limiter.Allow(vc, n)
}
