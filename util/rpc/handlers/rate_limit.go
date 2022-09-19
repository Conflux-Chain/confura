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

	ip, ok := GetIPAddressFromContext(ctx)
	if !ok { // ip is mandatory
		return true
	}

	// access token is optional
	token, _ := GetAccessTokenFromContext(ctx)

	vc := &rate.VisitContext{
		Ip: ip, Resource: name, Key: token,
	}

	limiter, ok := registry.Get(vc)
	if !ok {
		return true
	}

	return limiter.Allow(vc, n)
}
