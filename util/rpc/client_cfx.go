package rpc

import (
	"fmt"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/middleware"
	"github.com/conflux-chain/conflux-infura/util/metrics"
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
	o.MaxConnectionNum = maxConns
}

func MustNewCfxClientFromViper(options ...ClientOption) *sdk.Client {
	return MustNewCfxClient(cfxClientCfg.Http, options...)
}

func MustNewCfxWsClientFromViper(options ...ClientOption) *sdk.Client {
	return MustNewCfxClient(cfxClientCfg.WS, options...)
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
			RetryCount:       cfxClientCfg.Retry,
			RetryInterval:    cfxClientCfg.RetryInterval,
			RequestTimeout:   cfxClientCfg.RequestTimeout,
			MaxConnectionNum: cfxClientCfg.MaxConnsPerHost,
		},
	}

	for _, o := range options {
		o(opt)
	}

	cfx, err := sdk.NewClient(url, *opt.ClientOption)
	if err == nil && opt.hookMetrics {
		HookCfxRpcMetricsMiddleware(cfx)
	}

	return cfx, err
}

func HookCfxRpcMetricsMiddleware(cfx *sdk.Client) {
	cfx.UseCallRpcMiddleware(callRpcMetricsMiddleware)
}

func callRpcMetricsMiddleware(handler middleware.CallRpcHandler) middleware.CallRpcHandler {
	return middleware.CallRpcHandlerFunc(func(result interface{}, method string, args ...interface{}) error {
		start := time.Now()

		err := handler.Handle(result, method, args...)

		var metricKey string
		if err != nil {
			metricKey = fmt.Sprintf("infura/duration/cfx/rpc/call/%v/error", method)
		} else {
			metricKey = fmt.Sprintf("infura/duration/cfx/rpc/call/%v/success", method)
		}

		metrics.GetOrRegisterTimer(metricKey).UpdateSince(start)

		return err
	})
}
