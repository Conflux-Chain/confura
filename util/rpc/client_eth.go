package rpc

import (
	"fmt"
	"time"

	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/openweb3/web3go"
	providers "github.com/openweb3/web3go/provider_wrapper"
	"github.com/sirupsen/logrus"
)

type ethClientOption struct {
	baseClientOption
	*web3go.ClientOption
}

func (o *ethClientOption) SetRetryCount(retry int) {
	o.RetryCount = retry
}

func (o *ethClientOption) SetRetryInterval(retryInterval time.Duration) {
	o.RetryInterval = retryInterval
}

func (o *ethClientOption) SetRequestTimeout(reqTimeout time.Duration) {
	o.RequestTimeout = reqTimeout
}

func (o *ethClientOption) SetMaxConnsPerHost(maxConns int) {
	o.MaxConnectionNum = maxConns
}

func MustNewEthClientFromViper(options ...ClientOption) *web3go.Client {
	return MustNewEthClient(ethClientCfg.Http, options...)
}

func MustNewEthClient(url string, options ...ClientOption) *web3go.Client {
	eth, err := NewEthClient(url, options...)
	if err != nil {
		logrus.WithField("url", url).WithError(err).Fatal("Failed to create ETH client")
	}

	return eth
}

func NewEthClient(url string, options ...ClientOption) (*web3go.Client, error) {
	opt := ethClientOption{
		ClientOption: &web3go.ClientOption{
			RetryCount:       ethClientCfg.Retry,
			RetryInterval:    ethClientCfg.RetryInterval,
			RequestTimeout:   ethClientCfg.RequestTimeout,
			MaxConnectionNum: ethClientCfg.MaxConnsPerHost,
		},
	}

	for _, o := range options {
		o(&opt)
	}

	eth, err := web3go.NewClientWithOption(url, opt.ClientOption)
	if err == nil && opt.hookMetrics {
		HookEthRpcMetricsMiddleware(eth)
	}

	return eth, err
}

func HookEthRpcMetricsMiddleware(eth *web3go.Client) {
	mp := providers.NewMiddlewarableProvider(eth.Provider())
	mp.HookCall(callEthRpcMetricsMiddleware)
	eth.SetProvider(mp)
}

func callEthRpcMetricsMiddleware(f providers.CallFunc) providers.CallFunc {
	return providers.CallFunc(func(resultPtr interface{}, method string, args ...interface{}) error {
		start := time.Now()

		err := f(resultPtr, method, args...)

		var metricKey string
		if err != nil {
			metricKey = fmt.Sprintf("infura/duration/eth/rpc/call/%v/error", method)
		} else {
			metricKey = fmt.Sprintf("infura/duration/eth/rpc/call/%v/success", method)
		}

		metrics.GetOrRegisterTimer(metricKey).UpdateSince(start)

		return err
	})
}
