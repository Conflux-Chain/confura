package rpc

import (
	"time"

	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	"github.com/openweb3/web3go"
	"github.com/sirupsen/logrus"
)

type ethClientOption struct {
	baseClientOption
	web3go.ClientOption
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

func (o *ethClientOption) SetCircuitBreaker(maxFail int, failTimeWindow, openColdTime time.Duration) {
	o.WithCircuitBreaker(providers.DefaultCircuitBreakerOption{
		MaxFail:        maxFail,
		FailTimeWindow: failTimeWindow,
		OpenColdTime:   openColdTime,
	})
}

func MustNewEthClientsFromViper(options ...ClientOption) (clients []*web3go.Client) {
	for _, url := range ethClientCfg.Http {
		clients = append(clients, MustNewEthClient(url, options...))
	}
	return clients
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
		ClientOption: web3go.ClientOption{
			Option: providers.Option{
				RetryCount:           ethClientCfg.Retry,
				RetryInterval:        ethClientCfg.RetryInterval,
				RequestTimeout:       ethClientCfg.RequestTimeout,
				MaxConnectionPerHost: ethClientCfg.MaxConnsPerHost,
			},
		},
	}

	if cbConfig := ethClientCfg.CircuitBreaker; cbConfig.Enabled {
		opt.SetCircuitBreaker(cbConfig.MaxFail, cbConfig.FailTimeWindow, cbConfig.OpenColdTime)
	}

	for _, o := range options {
		o(&opt)
	}

	eth, err := web3go.NewClientWithOption(url, opt.ClientOption)
	if err != nil {
		return eth, err
	}

	hookFlag := MiddlewareHookAll
	if !opt.hookMetrics {
		hookFlag ^= MiddlewareHookLogMetrics
	}
	HookMiddlewares(eth.Provider(), url, "eth", hookFlag)

	return eth, nil
}
