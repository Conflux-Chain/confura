package rpc

import (
	"context"

	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	rpcprovider "github.com/openweb3/go-rpc-provider"
)

const pubSubLimitErrorCode = -32005

func reservePubSubSubscriptionPermit(ctx context.Context) (*rpcutil.PubSubSubscriptionPermit, error) {
	session, ok := rpcutil.PubSubSessionFromContext(ctx)
	if !ok {
		return nil, nil
	}

	permit, err := session.AcquireSubscription()
	if err != nil {
		return nil, &rpcprovider.JsonError{
			Code:    pubSubLimitErrorCode,
			Message: err.Error(),
		}
	}

	return permit, nil
}

func releasePubSubSubscriptionPermit(permit *rpcutil.PubSubSubscriptionPermit) {
	if permit != nil {
		permit.Release()
	}
}

// Create the provider subscription only after all fallible delegation work.
// The delegate uses its own ID so a failed request never enters serverSubs.
func newDelegateRPCSubscription(ctx context.Context, notifier *rpcprovider.Notifier,
	delegate *delegateSubscription, permit *rpcutil.PubSubSubscriptionPermit,
) (*rpcprovider.Subscription, error) {
	err := ctx.Err()
	if err == nil {
		select {
		case <-notifier.Closed():
			err = rpcprovider.ErrClientQuit
		case err = <-delegate.err:
			if err == nil {
				err = rpcprovider.ErrClientQuit
			}
		default:
		}
	}
	if err != nil {
		delegate.unsubscribe()
		releasePubSubSubscriptionPermit(permit)
		return nil, err
	}
	return notifier.CreateSubscription(), nil
}
