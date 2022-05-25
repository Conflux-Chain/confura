package rpc

import (
	"time"

	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	"github.com/openweb3/web3go"
	"github.com/sirupsen/logrus"
)

type ethClientOption struct {
	baseClientOption
	providers.Option
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
	o.MaxConnectionPerHost = maxConns
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
		Option: providers.Option{
			RetryCount:           ethClientCfg.Retry,
			RetryInterval:        ethClientCfg.RetryInterval,
			RequestTimeout:       ethClientCfg.RequestTimeout,
			MaxConnectionPerHost: ethClientCfg.MaxConnsPerHost,
		},
	}

	for _, o := range options {
		o(&opt)
	}

	eth, err := web3go.NewClientWithOption(url, opt.Option)
	if err == nil && opt.hookMetrics {
		HookEthRpcMetricsMiddleware(eth, url)
	}

	return eth, err
}

func HookEthRpcMetricsMiddleware(eth *web3go.Client, nodeUrl string) {
	mp := providers.NewMiddlewarableProvider(eth.Provider())
	mp.HookCallContext(func(cf providers.CallContextFunc) providers.CallContextFunc {
		return middlewareMetrics(nodeUrl, "eth", cf)
	})
	mp.HookCallContext(func(cf providers.CallContextFunc) providers.CallContextFunc {
		return middlewareLog(nodeUrl, "eth", cf)
	})
	eth.SetProvider(mp)
}
