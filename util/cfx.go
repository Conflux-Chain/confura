package util

import (
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// MustNewCfxClient creates an instance of CFX client or panic on error.
func MustNewCfxClient(url string) *sdk.Client {
	option := sdk.ClientOption{
		RetryCount:    viper.GetInt("cfx.retry"),
		RetryInterval: time.Millisecond * time.Duration(viper.GetInt("cfx.retryInterval")),
	}

	cfx, err := sdk.NewClient(url, option)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to create CFX client to %v", url)
	}

	return cfx
}
