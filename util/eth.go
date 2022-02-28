package util

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/openweb3/web3go"
	providers "github.com/openweb3/web3go/provider_wrapper"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	ethRpcCallSuccessMetricTimers sync.Map
	ethRpcCallErrorMetricCounters sync.Map
)

// MustNewEthClientFromViper creates an instance of ETH client or panic on error.
func MustNewEthClientFromViper() *web3go.Client {
	nodeUrl := viper.GetString("eth.http")
	return MustNewEthClient(nodeUrl)
}

// MustNewEthClient creates an instance of ETH client or panic on error.
func MustNewEthClient(url string) *web3go.Client {
	retryCount := viper.GetInt("cfx.retry")
	retryInterval := viper.GetDuration("cfx.retryInterval")
	requestTimeout := viper.GetDuration("cfx.requestTimeout")

	return MustNewEthClientWithRetry(url, retryCount, retryInterval, requestTimeout)
}

func MustNewEthClientWithRetry(
	url string, retry int, retryInterval, requestTimeout time.Duration,
) *web3go.Client {
	eth, err := web3go.NewClientWithOption(url, &web3go.ClientOption{
		RetryCount:     retry,
		RetryInterval:  retryInterval,
		RequestTimeout: requestTimeout,
	})

	if err != nil {
		logrus.WithError(err).Fatalf("Failed to create ETH client to %v", url)
	}

	HookEthRpcMetricsMiddleware(eth)

	return eth
}

func HookEthRpcMetricsMiddleware(eth *web3go.Client) {
	mp := providers.NewMiddlewarableProvider(eth.Provider())
	mp.HookCall(callEthRpcMetricsMiddleware)
	eth.SetProvider(mp)
}

func callEthRpcMetricsMiddleware(f providers.CallFunc) providers.CallFunc {
	metricFn := func(resultPtr interface{}, method string, args ...interface{}) error {
		start := time.Now()
		err := f(resultPtr, method, args...)
		duration := time.Since(start)

		if err != nil {
			// Update rpc call error counter metrics
			metricCounter, ok := ethRpcCallErrorMetricCounters.Load(method)
			if !ok {
				metricKey := fmt.Sprintf("infura/duration/eth/rpc/call/%v/error", method)
				metricCounter = metrics.GetOrRegisterCounter(metricKey, nil)
				ethRpcCallErrorMetricCounters.Store(method, metricCounter)
			}

			metricCounter.(metrics.Counter).Inc(1)
			return err
		}

		// Update rpc call success timer metrics
		metricTimer, ok := ethRpcCallSuccessMetricTimers.Load(method)
		if !ok {
			metricKey := fmt.Sprintf("infura/duration/eth/rpc/call/%v/success", method)
			metricTimer = metrics.GetOrRegisterTimer(metricKey, nil)
			ethRpcCallSuccessMetricTimers.Store(method, metricTimer)
		}

		metricTimer.(metrics.Timer).Update(duration)
		return nil
	}

	return providers.CallFunc(metricFn)
}
