package handlers

import (
	"context"
	"net/http"

	"github.com/conflux-chain/conflux-infura/util/rate"
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
	if !ok {
		return true
	}

	limiter, ok := registry.Get(name)
	if !ok {
		return true
	}

	return limiter.Allow(ip, n)
}
