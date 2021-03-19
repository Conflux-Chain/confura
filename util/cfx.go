package util

import (
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// MustNewCfxClient creates an instance of CFX client or panic on error.
func MustNewCfxClient(url string) *sdk.Client {
	retryCount := viper.GetInt("cfx.retry")
	retryInterval := time.Millisecond * time.Duration(viper.GetInt("cfx.retryInterval"))
	return MustNewCfxClientWithRetry(url, retryCount, retryInterval)
}

func MustNewCfxClientWithRetry(url string, retry int, retryInterval time.Duration) *sdk.Client {
	cfx, err := sdk.NewClient(url, sdk.ClientOption{
		RetryCount:    retry,
		RetryInterval: retryInterval,
	})

	if err != nil {
		logrus.WithError(err).Fatalf("Failed to create CFX client to %v", url)
	}

	return cfx
}
