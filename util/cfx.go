package util

import (
	"fmt"
	"sync"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/middleware"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	cfxRpcCallSuccessMetricTimers sync.Map
	cfxRpcCallErrorMetricCounters sync.Map
)

// MustNewCfxClient creates an instance of CFX client or panic on error.
func MustNewCfxClient(url string) *sdk.Client {
	retryCount := viper.GetInt("cfx.retry")
	retryInterval := time.Millisecond * time.Duration(viper.GetInt("cfx.retryInterval"))
	return MustNewCfxClientWithRetry(url, retryCount, retryInterval)
}

func MustNewCfxClientWithRetry(url string, retry int, retryInterval time.Duration) *sdk.Client {
	cfx, err := sdk.NewClient(url, sdk.ClientOption{
		RetryCount:    retry,
		RetryInterval: retryInterval,
	})

	if err != nil {
		logrus.WithError(err).Fatalf("Failed to create CFX client to %v", url)
	}

	cfx.UseCallRpcMiddleware(callRpcMetricsMiddleware)

	return cfx
}

func callRpcMetricsMiddleware(handler middleware.CallRpcHandler) middleware.CallRpcHandler {
	metricFn := func(result interface{}, method string, args ...interface{}) error {
		start := time.Now()
		err := handler.Handle(result, method, args...)
		duration := time.Since(start)

		if err != nil {
			// Update rpc call error counter metrics
			metricCounter, ok := cfxRpcCallErrorMetricCounters.Load(method)
			if !ok {
				metricKey := fmt.Sprintf("infura/duration/cfx/rpc/call/%v/error", method)
				metricCounter = metrics.GetOrRegisterCounter(metricKey, nil)
				cfxRpcCallErrorMetricCounters.Store(method, metricCounter)
			}

			metricCounter.(metrics.Counter).Inc(1)
			return err
		}

		// Update rpc call success timer metrics
		metricTimer, ok := cfxRpcCallSuccessMetricTimers.Load(method)
		if !ok {
			metricKey := fmt.Sprintf("infura/duration/cfx/rpc/call/%v/success", method)
			metricTimer = metrics.GetOrRegisterTimer(metricKey, nil)
			cfxRpcCallSuccessMetricTimers.Store(method, metricTimer)
		}

		metricTimer.(metrics.Timer).Update(duration)
		return nil
	}

	return middleware.CallRpcHandlerFunc(metricFn)
}
