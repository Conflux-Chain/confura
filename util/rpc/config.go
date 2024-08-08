package rpc

import (
	"time"

	"github.com/Conflux-Chain/go-conflux-util/viper"
)

var (
	cfxClientCfg clientConfig
	ethClientCfg clientConfig
)

type circuitBreakerConfig struct {
	Enabled        bool
	MaxFail        int           `default:"10"`
	FailTimeWindow time.Duration `default:"1s"`
	OpenColdTime   time.Duration `default:"15s"`
}

type clientConfig struct {
	Http            string
	Retry           int
	RetryInterval   time.Duration `default:"1s"`
	RequestTimeout  time.Duration `default:"3s"`
	MaxConnsPerHost int           `default:"1024"`
	CircuitBreaker  circuitBreakerConfig
}

type ClientOptioner interface {
	SetRetryCount(retry int)
	SetRetryInterval(retryInterval time.Duration)
	SetRequestTimeout(reqTimeout time.Duration)
	SetMaxConnsPerHost(maxConns int)
	SetHookMetrics(hook bool)
	SetCircuitBreaker(maxFail int, failTimeWindow, openColdTime time.Duration)
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

func WithCircuitBreaker(maxFail int, failTimeWindow, openColdTime time.Duration) ClientOption {
	return func(opt ClientOptioner) {
		opt.SetCircuitBreaker(maxFail, failTimeWindow, openColdTime)
	}
}

func MustInit() {
	viper.MustUnmarshalKey("cfx", &cfxClientCfg)
	viper.MustUnmarshalKey("eth", &ethClientCfg)
}
