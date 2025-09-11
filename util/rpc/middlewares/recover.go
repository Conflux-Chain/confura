package middlewares

import (
	"context"
	"errors"
	"runtime/debug"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

var (
	errMiddlewareCrashed = errors.New("RPC middleware crashed")
)

func Recover(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) (resp *rpc.JsonRpcMessage) {
		defer func() {
			if err := recover(); err != nil {
				// print stack info
				debug.PrintStack()

				// output RPC request context for diagnostics
				apiToken, _ := handlers.GetAccessTokenFromContext(ctx)
				ipAddr, _ := handlers.GetIPAddressFromContext(ctx)

				logrus.WithFields(logrus.Fields{
					"ipAddress": ipAddr,
					"apiToken":  apiToken,
				}).Info("RPC middleware panic with request context")

				// alert error message
				logrus.WithFields(logrus.Fields{
					"inputMsg": newHumanReadableRpcMessage(msg),
					"panicErr": err,
				}).Error("RPC middleware panic recovered")

				// rewrite error response
				resp = msg.ErrorResponse(errMiddlewareCrashed)
			}
		}()

		return next(ctx, msg)
	}
}

type humanReadableRpcMessage struct {
	Version string
	ID      string
	Method  string
	Params  string
	Error   error
	Result  string
}

func newHumanReadableRpcMessage(msg *rpc.JsonRpcMessage) *humanReadableRpcMessage {
	return &humanReadableRpcMessage{
		ID:      string(msg.ID),
		Version: msg.Version,
		Method:  msg.Method,
		Params:  string(msg.Params),
		Error:   msg.Error,
		Result:  string(msg.Result),
	}
}
