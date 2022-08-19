package middlewares

import (
	"context"
	"time"

	"github.com/Conflux-Chain/confura/util/rpc/handlers"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	web3pay "github.com/Conflux-Chain/web3pay-service/client"
	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type web3PayConfig struct {
	GatewayEndpoint string
	AppCoinContract string
	OwnerPrivateKey string
	RequestTimeout  time.Duration `default:"300ms"`
}

type BillingMwProviderOption struct {
	// fallback middleware for single or batch message call if billing failed
	Fallback      rpc.HandleCallMsgMiddleware
	BatchFallback rpc.HandleBatchMiddleware
}

type BillingMiddlewareProvider struct {
	*BillingMwProviderOption
	w3pClient *web3pay.Client
}

func MustNewBillingMiddlewareProviderFromViper(options ...BillingMwProviderOption) *BillingMiddlewareProvider {
	var config web3PayConfig
	viper.MustUnmarshalKey("web3pay", &config)

	// build billing key
	billingKey, err := web3pay.BuildBillingKey(config.AppCoinContract, config.OwnerPrivateKey)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to build billing key for web3pay middleware")
	}

	// new web3pay client
	client, err := web3pay.NewClientWithOption(config.GatewayEndpoint, web3pay.ClientOption{
		BillingKey: billingKey,
		Timeout:    config.RequestTimeout,
	})
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create web3pay client for new middleware")
	}

	var option *BillingMwProviderOption
	if len(options) > 0 {
		option = &options[0]
	}

	return &BillingMiddlewareProvider{
		BillingMwProviderOption: option,
		w3pClient:               client,
	}
}

func (p *BillingMiddlewareProvider) Billing(next rpc.HandleCallMsgFunc) rpc.HandleCallMsgFunc {
	// fallback handler if billing failed
	fbhandler := func(ctx context.Context, msg *rpc.JsonRpcMessage, err error) *rpc.JsonRpcMessage {
		logger := logrus.WithField("method", msg.Method).WithError(err)

		if p.Fallback != nil {
			logger.Debug("Billing middleware switching over to fallback middleware on failure")
			return p.Fallback(next)(ctx, msg)
		}

		logger.Debug("Billing middleware failed")
		return msg.ErrorResponse(err)
	}

	return func(ctx context.Context, msg *rpc.JsonRpcMessage) *rpc.JsonRpcMessage {
		consumerKey, ok := handlers.GetWeb3PayConsumerKeyFromContext(ctx)
		if !ok { // consumer key provided?
			err := errors.New("web3pay consumer key not provided")
			return fbhandler(ctx, msg, err)
		}

		receipt, err := p.w3pClient.Bill(ctx, msg.Method, false, consumerKey)
		if err != nil { // billing failed?
			return fbhandler(ctx, msg, errors.WithMessage(err, "web3pay billing failure"))
		}

		logrus.WithFields(logrus.Fields{
			"receipt": receipt,
			"method":  msg.Method,
		}).Debug("Billing middleware billed ok")
		return next(ctx, msg)
	}
}

func (p *BillingMiddlewareProvider) BillingBatch(next rpc.HandleBatchFunc) rpc.HandleBatchFunc {
	// fallback handler if billing failed
	fbhandler := func(ctx context.Context, msgs []*rpc.JsonRpcMessage, err error) []*rpc.JsonRpcMessage {
		if p.BatchFallback != nil {
			logrus.WithField("batch", len(msgs)).
				WithError(err).
				Debug("Billing middleware switching over to fallback middleware on failure")
			return p.BatchFallback(next)(ctx, msgs)
		}

		var responses []*rpc.JsonRpcMessage
		for _, v := range msgs {
			responses = append(responses, v.ErrorResponse(err))
		}

		return responses
	}

	return func(ctx context.Context, msgs []*rpc.JsonRpcMessage) []*rpc.JsonRpcMessage {
		consumerKey, ok := handlers.GetWeb3PayConsumerKeyFromContext(ctx)
		if !ok { // consumer key provided?
			err := errors.New("web3pay consumer key not provided")
			return fbhandler(ctx, msgs, err)
		}

		msgCalls := make(map[string]int64)
		for i := range msgs {
			msgCalls[msgs[i].Method]++
		}

		receipt, err := p.w3pClient.BillBatch(ctx, msgCalls, true, consumerKey)
		if err != nil { // batch billing failed?
			return fbhandler(ctx, msgs, errors.WithMessage(err, "web3pay billing failure"))
		}

		logrus.WithFields(logrus.Fields{
			"receipt": receipt,
			"batch":   len(msgs),
		}).Debug("Billing middleware billed ok")
		return next(ctx, msgs)
	}
}
