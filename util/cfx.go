package util

import (
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// MustNewCfxClient creates an instance of CFX client or panic on error.
func MustNewCfxClient(url string) *sdk.Client {
	retry := viper.GetInt("cfx.retry")
	interval := time.Duration(viper.GetInt("cfx.retryInterval"))

	cfx, err := sdk.NewClientWithRetry(url, retry, time.Millisecond*interval)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to create CFX client to %v", url)
	}

	return cfx
}
