package rpc

import (
	"context"

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

	ip, ok := util.GetIPAddress(ctx)
	if !ok {
		return nil
	}

	if limiter.Allow(ip, 1) {
		return nil
	}

	return errRateLimit
}
