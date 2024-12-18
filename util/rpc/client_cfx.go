package rpc

import (
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	"github.com/sirupsen/logrus"
)

type cfxClientOption struct {
	baseClientOption
	*sdk.ClientOption
}

func (o *cfxClientOption) SetRetryCount(retry int) {
	o.RetryCount = retry
}

func (o *cfxClientOption) SetRetryInterval(retryInterval time.Duration) {
	o.RetryInterval = retryInterval
}

func (o *cfxClientOption) SetRequestTimeout(reqTimeout time.Duration) {
	o.RequestTimeout = reqTimeout
}

func (o *cfxClientOption) SetMaxConnsPerHost(maxConns int) {
	o.MaxConnectionPerHost = maxConns
}

func (o *cfxClientOption) SetCircuitBreaker(maxFail int, failTimeWindow, openColdTime time.Duration) {
	o.CircuitBreakerOption = &providers.DefaultCircuitBreakerOption{
		MaxFail:        maxFail,
		FailTimeWindow: failTimeWindow,
		OpenColdTime:   openColdTime,
	}
}

func MustNewCfxClientsFromViper(options ...ClientOption) (clients []*sdk.Client) {
	for _, url := range cfxClientCfg.Http {
		clients = append(clients, MustNewCfxClient(url, options...))
	}
	return clients
}

func MustNewCfxClient(url string, options ...ClientOption) *sdk.Client {
	cfx, err := NewCfxClient(url, options...)
	if err != nil {
		logrus.WithField("url", url).WithError(err).Fatal("Failed to create CFX client")
	}

	return cfx
}

func NewCfxClient(url string, options ...ClientOption) (*sdk.Client, error) {
	opt := &cfxClientOption{
		ClientOption: &sdk.ClientOption{
			RetryCount:           cfxClientCfg.Retry,
			RetryInterval:        cfxClientCfg.RetryInterval,
			RequestTimeout:       cfxClientCfg.RequestTimeout,
			MaxConnectionPerHost: cfxClientCfg.MaxConnsPerHost,
		},
	}

	if cbConf := cfxClientCfg.CircuitBreaker; cbConf.Enabled {
		opt.SetCircuitBreaker(cbConf.MaxFail, cbConf.FailTimeWindow, cbConf.OpenColdTime)
	}

	for _, o := range options {
		o(opt)
	}

	cfx, err := sdk.NewClient(url, *opt.ClientOption)
	if err != nil {
		return cfx, err
	}

	hookFlag := MiddlewareHookAll
	if !opt.hookMetrics {
		hookFlag ^= MiddlewareHookLogMetrics
	}
	if !opt.hookCache {
		hookFlag ^= MiddlewareHookCache
	}
	HookMiddlewares(cfx.Provider(), url, "cfx", hookFlag)

	return cfx, nil
}
