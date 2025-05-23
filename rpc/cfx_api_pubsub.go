package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/util/metrics"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// PubSub notification
// TODO: restrict total sessions and sessions per IP, otherwise it maybe susceptible
// to flooding attack.

// NewHeads send a notification each time a new header (block) is appended to the chain.
func (api *cfxAPI) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
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

	headersCh := make(chan *types.BlockHeader, pubsubChannelBufferSize)
	dClient := getOrNewDelegateClient(psCtx.cfx)

	dSub, err := dClient.delegateSubscribeNewHeads(rpcSub.ID, headersCh)
	if err != nil {
		logrus.WithError(err).Info("Failed to delegate pubsub newheads subscription")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	nodeName := rpcutil.Url2NodeName(psCtx.cfx.GetNodeURL())
	counter := metrics.Registry.PubSub.Sessions("cfx", "new_heads", nodeName)
	counter.Inc(1)

	go func() {
		defer dSub.unsubscribe()
		defer counter.Dec(1)

		for {
			select {
			case blockHeader := <-headersCh:
				logrus.WithFields(logrus.Fields{
					"rpcSubID":    rpcSub.ID,
					"blockHeader": blockHeader,
				}).Debug("Received new block header from pubsub delegate")
				psCtx.notifier.Notify(rpcSub.ID, blockHeader)

			case err = <-dSub.err: // delegate subscription error
				logrus.WithField("rpcSubID", rpcSub.ID).WithError(err).Debug("Received error from newHeads pubsub delegate")
				psCtx.rpcClient.Close()
				return

			case err = <-rpcSub.Err():
				logrus.WithField("rpcSubID", rpcSub.ID).WithError(err).Debug("NewHeads pubsub subscription error")
				return

			case <-psCtx.notifier.Closed():
				logrus.WithField("rpcSubID", rpcSub.ID).Debug("NewHeads pubsub connection closed")
				return
			}
		}
	}()

	return rpcSub, nil
}

// Epochs send a notification each time a new epoch is appended to the chain.
func (api *cfxAPI) Epochs(ctx context.Context, subEpoch *types.Epoch) (*rpc.Subscription, error) {
	if subEpoch == nil {
		subEpoch = types.EpochLatestMined
	}

	if !subEpoch.Equals(types.EpochLatestMined) && !subEpoch.Equals(types.EpochLatestState) {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	psCtx, supported, err := api.pubsubCtxFromContext(ctx)
	if err != nil {
		logrus.WithError(err).Infof("Epochs pubsub context error (%v)", subEpoch)
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	if !supported {
		logrus.WithError(err).Infof("Epochs pubsub notification unsupported (%v)", subEpoch)
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := psCtx.notifier.CreateSubscription()

	epochsCh := make(chan *types.WebsocketEpochResponse, pubsubChannelBufferSize)
	dClient := getOrNewDelegateClient(psCtx.cfx)

	dSub, err := dClient.delegateSubscribeEpochs(rpcSub.ID, epochsCh, *subEpoch)
	if err != nil {
		logrus.WithField("subEpoch", subEpoch).
			WithError(err).
			Info("Failed to delegate pubsub epochs subscription")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	nodeName := rpcutil.Url2NodeName(psCtx.cfx.GetNodeURL())
	counter := metrics.Registry.PubSub.Sessions("cfx", "epochs", nodeName)
	counter.Inc(1)

	go func() {
		defer dSub.unsubscribe()
		defer counter.Dec(1)

		for {
			select {
			case epoch := <-epochsCh:
				logrus.WithFields(logrus.Fields{
					"rpcSubID": rpcSub.ID,
					"epoch":    epoch,
				}).Debugf("Received new epoch from pubsub delegate (%v)", subEpoch)
				psCtx.notifier.Notify(rpcSub.ID, epoch)

			case err = <-dSub.err: // delegate subscription error
				logrus.WithField("rpcSubID", rpcSub.ID).WithError(err).Debugf("Received error from epochs pubsub delegate (%v)", subEpoch)
				psCtx.rpcClient.Close()
				return

			case err = <-rpcSub.Err():
				logrus.WithField("rpcSubID", rpcSub.ID).WithError(err).Debugf("Epochs pubsub subscription error (%v)", subEpoch)
				return

			case <-psCtx.notifier.Closed():
				logrus.WithField("rpcSubID", rpcSub.ID).Debugf("Epochs pubsub connection closed (%v)", subEpoch)
				return
			}
		}
	}()

	return rpcSub, nil
}

// Logs creates a subscription that fires for all new log that match the given filter criteria.
func (api *cfxAPI) Logs(ctx context.Context, filter types.LogFilter) (*rpc.Subscription, error) {
	metrics.Registry.PubSub.InputLogFilter("cfx").Mark(!isEmptyLogFilter(filter))

	psCtx, supported, err := api.pubsubCtxFromContext(ctx)
	if err != nil {
		logrus.WithError(err).Info("Logs pubsub context error")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	if !supported {
		logrus.WithError(err).Info("Logs pubsub notification unsupported")
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := psCtx.notifier.CreateSubscription()

	logsCh := make(chan *types.SubscriptionLog, pubsubChannelBufferSize)
	dClient := getOrNewDelegateClient(psCtx.cfx)

	dSub, err := dClient.delegateSubscribeLogs(rpcSub.ID, logsCh, filter)
	if err != nil {
		logrus.WithField("filter", filter).
			WithError(err).
			Info("Failed to delegate pubsub logs subscription")
		return &rpc.Subscription{}, errSubscriptionProxyError
	}

	nodeName := rpcutil.Url2NodeName(psCtx.cfx.GetNodeURL())
	counter := metrics.Registry.PubSub.Sessions("cfx", "logs", nodeName)
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

type pubsubContext struct {
	notifier  *rpc.Notifier
	rpcClient *rpc.Client
	cfx       sdk.ClientOperator
}

// pubsubCtxFromContext returns the pubsub context with member variables stored in ctx, if any.
func (api *cfxAPI) pubsubCtxFromContext(ctx context.Context) (psCtx *pubsubContext, supported bool, err error) {
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

	cfx, err := api.provider.GetClientByIP(ctx, node.GroupCfxWs)
	if err != nil {
		err = errors.WithMessage(err, "failed to get cfx wsclient by ip")
		return
	}

	psCtx = &pubsubContext{notifier, rpcClient, cfx}
	return
}
