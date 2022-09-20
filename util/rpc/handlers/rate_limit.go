package handlers

import (
	"context"
	"net"
	"net/http"

	"github.com/scroll-tech/rpc-gateway/util/rate"
)

func RateLimit(registry *rate.Registry) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), CtxKeyRateRegistry, registry)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func WhiteListAllow(ctx context.Context) bool {
	if ip, ok := GetIPAddressFromContext(ctx); ok {
		nip := net.ParseIP(ip)

		// always allow request from loopback ip
		if nip != nil && nip.IsLoopback() {
			return true
		}
	}

	registry, ok := ctx.Value(CtxKeyRateRegistry).(*rate.Registry)
	if !ok {
		return false
	}

	if token, ok := GetAccessTokenFromContext(ctx); ok {
		return registry.WhiteListed(token)
	}

	return false
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
