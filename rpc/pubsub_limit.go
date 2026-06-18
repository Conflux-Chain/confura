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
