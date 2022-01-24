package util

import (
	"time"

	"github.com/openweb3/web3go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// MustNewEthClientFromViper creates an instance of ETH client or panic on error.
func MustNewEthClientFromViper() *web3go.Client {
	nodeUrl := viper.GetString("eth.http")
	retryCount := viper.GetInt("eth.retry")
	retryInterval := viper.GetDuration("eth.retryInterval")
	requestTimeout := viper.GetDuration("eth.requestTimeout")

	return MustNewEthClientWithRetry(nodeUrl, retryCount, retryInterval, requestTimeout)
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

	return eth
}
