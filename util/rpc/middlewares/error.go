package middlewares

import (
	"context"
	"errors"
	"strings"

	"github.com/openweb3/go-rpc-provider"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
)

var (
	// The following nginx gateway error will be uniformed to be more human-readable:
	// 502 Bad Gateway: invalid response from the upstream server.
	// 503 Service Unavailable: unable to handle the request due to a temporary overload or maintenance.
	// 504 Gateway Timeout: failed to receive a timely response from the upstream server.
	nginxUnavailableErrorPatterns = []string{
		"502 bad gateway", "503 service unavailable", "504 gateway timeout",
	}

	ErrorServerTooBusy = errors.New("server is too busy, please try again later")
)

func matchNginxUnavailableError(err error) bool {
	errMsg := strings.ToLower(err.Error())
	for _, pattern := range nginxUnavailableErrorPatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

func isServerTooBusy(err error) bool {
	return matchNginxUnavailableError(err) || errors.Is(err, providers.ErrCircuitOpen)
}

func UniformError(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		resp := next(ctx, msg)
		if resp.Error != nil && isServerTooBusy(resp.Error) {
			return resp.ErrorResponse(ErrorServerTooBusy)
		}

		return resp
	}
}
