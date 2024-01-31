package middlewares

import (
	"strings"

	// ensure viper based configuration initialized at the very beginning
	_ "github.com/Conflux-Chain/confura/config"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	web3pay "github.com/Conflux-Chain/web3pay-service/client"
	web3paymw "github.com/Conflux-Chain/web3pay-service/client/middleware"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

const (
	Web3PayBillingMode      = "billing"
	Web3PaySubscriptionMode = "subscription"
)

type web3payConfig struct {
	Enabled      bool
	Mode         string
	Billing      web3pay.BillingClientConfig
	Subscription web3pay.VipSubscriptionClientConfig
}

func (conf *web3payConfig) IsBillingMode() bool {
	return strings.EqualFold(conf.Mode, Web3PayBillingMode)
}

func (conf *web3payConfig) IsSubscriptionMode() bool {
	return strings.EqualFold(conf.Mode, Web3PaySubscriptionMode)
}

func MustNewWeb3PayMiddlewareFromViper() (rpc.HandleCallMsgMiddleware, *web3payConfig, bool) {
	conf := mustNewWeb3payConfigFromViper()

	if !conf.Enabled {
		return nil, nil, false
	}

	var mw rpc.HandleCallMsgMiddleware
	if conf.IsSubscriptionMode() {
		mw = mustNewWeb3PayVIpSubscriptionMiddleware(conf)
	} else {
		mw = mustNewWeb3PayBillingMiddleware(conf)
	}

	return mw, conf, true
}

func mustNewWeb3PayVIpSubscriptionMiddleware(conf *web3payConfig) rpc.HandleCallMsgMiddleware {
	client, err := web3pay.NewVipSubscriptionClient(conf.Subscription)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new Web3Pay VIP subscription client")
	}

	mwoption := web3paymw.NewVipSubscriptionMiddlewareOptionWithClient(client)
	mwoption.ApiKeyProvider = handlers.GetAccessTokenFromContext
	return web3paymw.Openweb3VipSubscriptionMiddleware(mwoption)
}

func mustNewWeb3PayBillingMiddleware(conf *web3payConfig) rpc.HandleCallMsgMiddleware {
	client, err := web3pay.NewBillingClient(conf.Billing)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to new Web3Pay billing client")
	}

	mwoption := web3paymw.NewOw3BillingMiddlewareOptionWithClient(client)
	mwoption.ApiKeyProvider = handlers.GetAccessTokenFromContext
	return web3paymw.Openweb3BillingMiddleware(mwoption)
}

func mustNewWeb3payConfigFromViper() *web3payConfig {
	var conf web3payConfig
	viper.MustUnmarshalKey("web3pay", &conf)

	if conf.Enabled {
		// validate config mode
		if !strings.EqualFold(conf.Mode, Web3PayBillingMode) &&
			!strings.EqualFold(conf.Mode, Web3PaySubscriptionMode) {
			logrus.WithField("mode", conf.Mode).Fatal("Invalid web3pay payment mode configured")
		}
	}

	return &conf
}
