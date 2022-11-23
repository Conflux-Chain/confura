package middlewares

import (
	"context"
	"errors"
	"regexp"

	"github.com/openweb3/go-rpc-provider"
)

var (
	// regex to validate RPC method format such as "cfx_call"
	rpcMethodValidationRegex, _ = regexp.Compile("^[[:alpha:]]{3}_[[:alpha:]]+$")

	errInvalidRpcMethod = errors.New("invalid JSON-RPC method")
)

func AntiInjection(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		if !rpcMethodValidationRegex.Match([]byte(msg.Method)) {
			return msg.ErrorResponse(errInvalidRpcMethod)
		}

		return next(ctx, msg)
	}
}
