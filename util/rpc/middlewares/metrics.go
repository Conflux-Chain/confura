package middlewares

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/Conflux-Chain/confura/util/rpc/handlers"
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

		metricMethod := msg.Method
		if resp.Error != nil && isMethodNotFoundByError(msg.Method, resp.Error) {
			metricMethod = "method_not_found"
		}

		// collect rpc QPS/latency etc.
		metrics.Registry.RPC.UpdateDuration(metricMethod, unwrapJsonError(resp.Error), start)
		// collect traffic hits
		metrics.DefaultTrafficCollector().MarkHit(getTrafficSourceFromContext(ctx))

		return resp
	}
}

func getTrafficSourceFromContext(ctx context.Context) (source string) {
	if authId, ok := handlers.GetAuthIdFromContext(ctx); ok {
		source = authId
	} else {
		source, _ = handlers.GetIPAddressFromContext(ctx)
	}

	if len(source) == 0 {
		source = "unknown_source"
	}

	return source
}

func isMethodNotFoundByError(method string, err error) bool {
	subPattern := fmt.Sprintf("the method %s does not exist/is not available", method)
	return strings.Contains(err.Error(), subPattern)
}

func unwrapJsonError(jsErr *rpc.JsonError) error {
	if jsErr == nil {
		return nil
	}

	if err := jsErr.Inner(); err != nil {
		return err
	}

	return jsErr
}
