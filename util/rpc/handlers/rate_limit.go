package handlers

import (
	"context"
	"net/http"

	"github.com/Conflux-Chain/confura/util/rate"
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

	vc := &rate.VistingContext{Resource: name}

	if ip, ok := GetIPAddressFromContext(ctx); ok { // ip
		vc.Ip = ip
	}

	if token, ok := GetAccessTokenFromContext(ctx); ok { // key
		vc.Key = token
	}

	throttler, ok := registry.Get(vc)
	if !ok {
		return true
	}

	return throttler.Allow(vc, n)
}
