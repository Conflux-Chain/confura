package rpc

import (
	"time"

	"github.com/Conflux-Chain/go-conflux-util/viper"
)

var (
	cfxClientCfg clientConfig
	ethClientCfg clientConfig
)

type clientConfig struct {
	WS              string
	Http            string
	Retry           int           `default:"3"`
	RetryInterval   time.Duration `default:"1s"`
	RequestTimeout  time.Duration `default:"3s"`
	MaxConnsPerHost int           `default:"1024"`
}

type ClientOptioner interface {
	SetRetryCount(retry int)
	SetRetryInterval(retryInterval time.Duration)
	SetRequestTimeout(reqTimeout time.Duration)
	SetMaxConnsPerHost(maxConns int)
	SetHookMetrics(hook bool)
}

type baseClientOption struct {
	hookMetrics bool
}

func (o *baseClientOption) SetHookMetrics(hook bool) {
	o.hookMetrics = hook
}

type ClientOption func(opt ClientOptioner)

func WithClientRetryCount(retry int) ClientOption {
	return func(opt ClientOptioner) {
		opt.SetRetryCount(retry)
	}
}

func WithClientRequestTimeout(reqTimeout time.Duration) ClientOption {
	return func(opt ClientOptioner) {
		opt.SetRequestTimeout(reqTimeout)
	}
}

func WithClientRetryInterval(retryInterval time.Duration) ClientOption {
	return func(opt ClientOptioner) {
		opt.SetRetryInterval(retryInterval)
	}
}

func WithClientMaxConnsPerHost(maxConns int) ClientOption {
	return func(opt ClientOptioner) {
		opt.SetMaxConnsPerHost(maxConns)
	}
}

func WithClientHookMetrics(hook bool) ClientOption {
	return func(opt ClientOptioner) {
		opt.SetHookMetrics(hook)
	}
}

func Init() {
	viper.MustUnmarshalKey("cfx", &cfxClientCfg)
	viper.MustUnmarshalKey("eth", &ethClientCfg)
}
