package rpc

import (
	"context"
	"net/http"

	"github.com/conflux-chain/conflux-infura/util"
	"github.com/conflux-chain/conflux-infura/util/rate"
	"github.com/pkg/errors"
)

var (
	errRateLimit = errors.New("too many requests")

	// TODO Implement rate limit in interceptor way.
)

func validateRateLimit(ctx context.Context, registry *rate.Registry, method string) error {
	limiter, ok := registry.Get(method)
	if !ok {
		return nil
	}

	request, ok := ctx.Value("request").(*http.Request)
	if !ok {
		// do not support to throttle requests from websocket
		return nil
	}

	ip := util.GetIPAddress(request)

	if limiter.Allow(ip, 1) {
		return nil
	}

	return errRateLimit
}
