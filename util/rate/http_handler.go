package rate

import (
	"context"
	"net/http"

	"github.com/conflux-chain/conflux-infura/util"
)

type registryKey struct{}

func HttpHandler(registry *Registry, next http.Handler) http.Handler {
	if registry == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		ip, ok := util.GetIPAddress(ctx)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}

		limiter, ok := registry.Get("rpc.httpRequest")
		if ok && !limiter.Allow(ip, 1) {
			http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
			return
		}

		// inject rate limiter registry to throttle RPCs
		ctx = context.WithValue(ctx, registryKey{}, registry)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func Allow(ctx context.Context, name string, n int) bool {
	registry, ok := ctx.Value(registryKey{}).(*Registry)
	if !ok {
		return true
	}

	ip, ok := util.GetIPAddress(ctx)
	if !ok {
		return true
	}

	limiter, ok := registry.Get(name)
	if !ok {
		return true
	}

	return limiter.Allow(ip, n)
}
