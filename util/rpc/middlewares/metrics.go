package middlewares

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/openweb3/go-rpc-provider"
)

func MetricsBatch(next rpc.HandleBatchFunc) rpc.HandleBatchFunc {
	return func(ctx context.Context, msgs []*rpc.JsonRpcMessage) []*rpc.JsonRpcMessage {
		start := time.Now()
		resp := next(ctx, msgs)

		metrics.Registry.RPC.BatchLatency().Update(time.Since(start).Nanoseconds())
		metrics.Registry.RPC.BatchSize().Update(int64(len(msgs)))

		return resp
	}
}

func Metrics(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		start := time.Now()
		resp := next(ctx, msg)

		mmethod := msg.Method
		if isMethodNotFoundByError(msg.Method, resp.Error) {
			mmethod = "method_not_found"
		}

		metrics.Registry.RPC.UpdateDuration(mmethod, resp.Error, start)
		return resp
	}
}

func isMethodNotFoundByError(method string, err error) bool {
	subpattern := fmt.Sprintf("the method %s does not exist/is not available", method)
	return strings.Contains(err.Error(), subpattern)
}
