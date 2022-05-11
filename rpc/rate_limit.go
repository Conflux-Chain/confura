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

	// TODO
	// 1. Load rate limit configurations from DB or file.
	// 2. Support to update at runtime.
	// 3. Implement rate limit in interceptor way.

	rateLimitCfxLogs      = rate.DefaultRegistry.GetOrRegister("cfx_getLogs", 2, 10)
	rateLimitCfxCall      = rate.DefaultRegistry.GetOrRegister("cfx_call", 5, 50)
	rateLimitCfxSendTx    = rate.DefaultRegistry.GetOrRegister("cfx_sendRawTransaction", 1, 20)
	rateLimitCfxNextNonce = rate.DefaultRegistry.GetOrRegister("cfx_getNextNonce", 1, 20)

	rateLimitEthLogs          = rate.DefaultRegistry.GetOrRegister("eth_getLogs", 2, 10)
	rateLimitEthCall          = rate.DefaultRegistry.GetOrRegister("eth_call", 5, 50)
	rateLimitEthSendTx        = rate.DefaultRegistry.GetOrRegister("eth_sendRawTransaction", 1, 20)
	rateLimitEthNextNonce     = rate.DefaultRegistry.GetOrRegister("eth_getNextNonce", 1, 20)
	rateLimitEthBlockByNumber = rate.DefaultRegistry.GetOrRegister("eth_getBlockByNumber", 5, 50)
)

func validateRateLimit(ctx context.Context, limiter *rate.IpLimiter) error {
	request := ctx.Value("request").(*http.Request)
	ip := util.GetIPAddress(request)

	if limiter.Allow(ip, 1) {
		return nil
	}

	return errRateLimit
}
