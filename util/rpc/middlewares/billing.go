package middlewares

import (
	"time"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	web3pay "github.com/Conflux-Chain/web3pay-service/client"
	"github.com/sirupsen/logrus"
)

type web3PayConfig struct {
	Enabled         bool
	BillingKey      string
	GatewayEndpoint string
	RequestTimeout  time.Duration `default:"1s"`
}

func MustNewWeb3PayMiddlewareProvider() (*web3pay.RpcMiddlewareProvider, bool) {
	var config web3PayConfig
	viper.MustUnmarshalKey("web3pay", &config)

	if !config.Enabled {
		logrus.Info("Web3pay billing RPC middleware not enabled")
		return nil, false
	}

	provider, err := web3pay.NewRpcMiddlewareProvider(
		config.GatewayEndpoint,
		web3pay.RpcMiddlewareProviderOption{
			Timeout:     config.RequestTimeout,
			BillingKey:  config.BillingKey,
			CustomerKey: handlers.GetAccessTokenFromContext,
			// rate limit is used as fallback for billing middleware.
			BillingFallbackMw:      RateLimit,
			BillingBatchFallbackMw: RateLimitBatch,
		},
	)

	if err != nil {
		logrus.WithError(err).Fatal("Failed to new Web3Pay RPC middleware provider")
	}

	return provider, true
}
