package util

import (
	"fmt"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/middleware"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// MustNewCfxClient creates an instance of CFX client or panic on error.
func MustNewCfxClient(url string) *sdk.Client {
	retryCount := viper.GetInt("cfx.retry")
	retryInterval := viper.GetDuration("cfx.retryInterval")
	requestTimeout := viper.GetDuration("cfx.requestTimeout")

	return MustNewCfxClientWithRetry(url, retryCount, retryInterval, requestTimeout)
}

func MustNewCfxClientWithRetry(url string, retry int, retryInterval, requestTimeout time.Duration) *sdk.Client {
	cfx, err := sdk.NewClient(url, sdk.ClientOption{
		RetryCount:     retry,
		RetryInterval:  retryInterval,
		RequestTimeout: requestTimeout,
	})

	if err != nil {
		logrus.WithError(err).Fatalf("Failed to create CFX client to %v", url)
	}

	HookCfxRpcMetricsMiddleware(cfx)

	return cfx
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
