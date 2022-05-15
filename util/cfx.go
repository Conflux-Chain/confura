package util

import (
	"fmt"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/middleware"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/metrics"
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

func HookCfxRpcConsoleLogMiddleware(cfx *sdk.Client) {
	cfx.UseCallRpcMiddleware(middleware.CallRpcConsoleMiddleware)
	cfx.UseBatchCallRpcMiddleware(middleware.BatchCallRpcConsoleMiddleware)
}

func callRpcMetricsMiddleware(handler middleware.CallRpcHandler) middleware.CallRpcHandler {
	metricFn := func(result interface{}, method string, args ...interface{}) error {
		start := time.Now()

		var metricKey string
		err := handler.Handle(result, method, args...)

		if err != nil {
			metricKey = fmt.Sprintf("infura/duration/cfx/rpc/call/%v/error", method)
		} else {
			metricKey = fmt.Sprintf("infura/duration/cfx/rpc/call/%v/success", method)
		}

		metricTimer := metrics.GetOrRegisterTimer(metricKey, nil)
		metricTimer.UpdateSince(start)

		return err
	}

	return middleware.CallRpcHandlerFunc(metricFn)
}

func IsTxExecutedInBlock(tx *types.Transaction) bool {
	return tx != nil && tx.BlockHash != nil && tx.Status != nil && *tx.Status < 2
}

// IsEmptyBlock checks if block contains any executed transaction(s)
func IsEmptyBlock(block *types.Block) bool {
	for _, tx := range block.Transactions {
		if IsTxExecutedInBlock(&tx) {
			return false
		}
	}

	return true
}
