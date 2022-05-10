package util

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/openweb3/web3go"
	providers "github.com/openweb3/web3go/provider_wrapper"
	"github.com/openweb3/web3go/types"
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
	retryCount := viper.GetInt("eth.retry")
	retryInterval := viper.GetDuration("eth.retryInterval")
	requestTimeout := viper.GetDuration("eth.requestTimeout")

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

		var metricKey string
		if err := f(resultPtr, method, args...); err != nil {
			metricKey = fmt.Sprintf("infura/duration/eth/rpc/call/%v/error", method)
		} else {
			metricKey = fmt.Sprintf("infura/duration/eth/rpc/call/%v/success", method)
		}

		metricTimer := metrics.GetOrRegisterTimer(metricKey, nil)
		metricTimer.UpdateSince(start)

		return nil
	}

	return providers.CallFunc(metricFn)
}

// IsEip155Tx check if the EVM transaction is compliant to EIP155
func IsEip155Tx(tx *types.Transaction) bool {
	if tx.V != nil && tx.V.Uint64() >= 35 {
		return true
	}

	return false
}

// IsLegacyEthTx check if the EVM transaction is legacy (pre EIP155)
func IsLegacyEthTx(tx *types.Transaction) bool {
	if tx.V != nil && tx.V.Uint64() == 27 || tx.V.Uint64() == 28 {
		return true
	}

	return false
}
