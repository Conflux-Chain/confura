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

		metricMethod := registeredMetricMethod(ctx, msg)
		if resp == nil {
			return nil
		}

		// collect rpc QPS/latency, response size etc.
		space, _ := handlers.GetNamespaceFromContext(ctx)
		metrics.Registry.RPC.UpdateDuration(space, metricMethod, unwrapJsonError(resp.Error), start)
		if resp.Error == nil {
			metrics.Registry.RPC.ResponseSize(space, metricMethod).Update(int64(len(resp.Result)))
		}

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

// Classify against registered callbacks, independently of early routing/auth
// errors. Never use arbitrary client-supplied method names as metric labels.
func registeredMetricMethod(ctx context.Context, msg *rpc.JsonRpcMessage) string {
	switch msg.Method {
	case "eth_subscribe", "eth_unsubscribe", "cfx_subscribe", "cfx_unsubscribe":
		return msg.Method
	}
	if h, ok := ctx.Value("handler").(*rpc.ConnHandler); ok {
		_, err := h.ParseCallArguments(msg)
		if err == nil {
			return msg.Method
		}
		if e, ok := err.(rpc.Error); ok && e.ErrorCode() != -32601 {
			return msg.Method
		}
	}
	return "method_not_found"
}
