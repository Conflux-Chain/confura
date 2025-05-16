package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/util/metrics"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// eSpace PubSub notification
// TODO:
// 1. restrict total sessions and sessions per IP, otherwise it maybe susceptible
// to flooding attack;
// 2. `newPendingTransactions` and `syncing` are not implemented in the fullnode yet.

// NewHeads send a notification each time a new header (block) is appended to the chain.
func (api *ethAPI) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	psCtx, supported, err := api.pubsubCtxFromContext(ctx)
	if err != nil {
		logrus.WithError(err).Info("NewHeads pubsub context error")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	if !supported {
		logrus.WithError(err).Info("NewHeads pubsub notification unsupported")
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := psCtx.notifier.CreateSubscription()

	headersCh := make(chan *types.Header, pubsubChannelBufferSize)
	dClient := getOrNewEthDelegateClient(psCtx.eth)

	dSub, err := dClient.delegateSubscribeNewHeads(rpcSub.ID, headersCh)
	if err != nil {
		logrus.WithError(err).Info("Failed to delegate pubsub newheads subscription")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	logger := logrus.WithField("rpcSubID", rpcSub.ID)

	nodeName := rpcutil.Url2NodeName(psCtx.eth.URL)
	counter := metrics.Registry.PubSub.Sessions("eth", "new_heads", nodeName)
	counter.Inc(1)

	go func() {
		defer dSub.unsubscribe()
		defer counter.Dec(1)

		for {
			select {
			case blockHeader := <-headersCh:
				logger.WithField("blockHeader", blockHeader).Debug("Received new block header from pubsub delegate")
				psCtx.notifier.Notify(rpcSub.ID, blockHeader)

			case err = <-dSub.err: // delegate subscription error
				logger.WithError(err).Debug("Received error from newHeads pubsub delegate")
				psCtx.rpcClient.Close()
				return

			case err = <-rpcSub.Err(): // client connection closed or error
				logger.WithError(err).Debug("NewHeads pubsub subscription error")
				return

			case <-psCtx.notifier.Closed():
				logger.Debug("NewHeads pubsub connection closed")
				return
			}
		}
	}()

	return rpcSub, nil
}

// Logs creates a subscription that fires for all new log that match the given filter criteria.
func (api *ethAPI) Logs(ctx context.Context, filter types.FilterQuery) (*rpc.Subscription, error) {
	metrics.Registry.PubSub.InputLogFilter("eth").Mark(!isEmptyEthLogFilter(filter))

	psCtx, supported, err := api.pubsubCtxFromContext(ctx)
	if err != nil {
		logrus.WithError(err).Error("Logs pubsub context error")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	if !supported {
		logrus.WithError(err).Error("Logs pubsub notification unsupported")
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := psCtx.notifier.CreateSubscription()

	logsCh := make(chan *types.Log, pubsubChannelBufferSize)
	dClient := getOrNewEthDelegateClient(psCtx.eth)

	dSub, err := dClient.delegateSubscribeLogs(rpcSub.ID, logsCh, filter)
	if err != nil {
		logrus.WithField("filter", filter).WithError(err).Info("Failed to delegate pubsub logs subscription")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	nodeName := rpcutil.Url2NodeName(psCtx.eth.URL)
	counter := metrics.Registry.PubSub.Sessions("eth", "logs", nodeName)
	counter.Inc(1)

	go func() {
		defer dSub.unsubscribe()
		defer counter.Dec(1)

		for {
			select {
			case log := <-logsCh:
				logrus.WithFields(logrus.Fields{
					"rpcSubID": rpcSub.ID,
					"log":      log,
				}).Debug("Received new log from pubsub delegate")
				psCtx.notifier.Notify(rpcSub.ID, log)

			case err = <-dSub.err: // delegate subscription error
				logrus.WithField("rpcSubID", rpcSub.ID).WithError(err).Debug("Received error from logs pubsub delegate")
				psCtx.rpcClient.Close()
				return

			case err = <-rpcSub.Err():
				logrus.WithField("rpcSubID", rpcSub.ID).WithError(err).Debugf("Logs pubsub subscription error")
				return

			case <-psCtx.notifier.Closed():
				logrus.WithField("rpcSubID", rpcSub.ID).Debugf("Logs pubsub connection closed")
				return
			}
		}
	}()

	return rpcSub, nil
}

type epubsubContext struct {
	notifier  *rpc.Notifier
	rpcClient *rpc.Client
	eth       *node.Web3goClient
}

// pubsubCtxFromContext returns the pubsub context with member variables stored in ctx, if any.
func (api *ethAPI) pubsubCtxFromContext(ctx context.Context) (psCtx *epubsubContext, supported bool, err error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		err = errors.New("failed to get notifier from context")
		return
	}

	rpcClient, supported := rpcClientFromContext(ctx)
	if !supported {
		err = errors.New("failed to get rpc client from context")
		return
	}

	eth, err := api.provider.GetClientByIP(ctx, node.GroupEthWs)
	if err != nil {
		err = errors.WithMessage(err, "failed to get eth wsclient by ip")
		return
	}

	psCtx = &epubsubContext{notifier, rpcClient, eth}
	return
}
