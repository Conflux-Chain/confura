package rpc

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	"github.com/scroll-tech/rpc-gateway/node"
	"github.com/scroll-tech/rpc-gateway/util"
	rpcutil "github.com/scroll-tech/rpc-gateway/util/rpc"
	"github.com/sirupsen/logrus"
)

// ethDelegateClient eth client delegated for pubsub subscription
type ethDelegateClient struct {
	*node.Web3goClient

	delegateContexts util.ConcurrentMap // context name => *delegateContext
}

func getOrNewEthDelegateClient(eth *node.Web3goClient) *ethDelegateClient {
	nodeName := rpcutil.Url2NodeName(eth.URL)
	client, _ := delegateClients.LoadOrStore(nodeName, &ethDelegateClient{Web3goClient: eth})
	return client.(*ethDelegateClient)
}

func (client *ethDelegateClient) getDelegateCtx(ctxName string) *delegateContext {
	dctx, _ := client.delegateContexts.LoadOrStore(ctxName, newDelegateContext())
	return dctx.(*delegateContext)
}

func (client *ethDelegateClient) delegateSubscribeNewHeads(subId rpc.ID, channel chan *types.Header) (*delegateSubscription, error) {
	dCtx := client.getDelegateCtx(nhCtxName)
	if dCtx.getStatus() == delegateStatusErr {
		return nil, errDelegateNotReady
	}

	delegateSub := dCtx.registerDelegateSub(subId, channel)
	dCtx.run(client.proxySubscribeNewHeads)

	return delegateSub, nil
}

func (client *ethDelegateClient) proxySubscribeNewHeads(dctx *delegateContext) {
	subFunc := func() (types.Subscription, chan *types.Header, error) {
		nhCh := make(chan *types.Header, pubsubChannelBufferSize)
		sub, err := client.Eth.SubscribeNewHead(nhCh)
		return sub, nhCh, err
	}

	logger := logrus.WithField("nodeURL", client.URL)

	for {
		csub, nhCh, err := subFunc()
		for failures := 0; err != nil; { // resub until suceess
			logger.WithError(err).Info("NewHead proxy subscriptions error")

			if failures++; failures%maxProxyDelegateSubFailures == 0 {
				// trigger error for every few failures
				logger.WithField("failures", failures).
					WithError(err).Error("Failed to try too many newHeads proxy subscriptions")
			}

			time.Sleep(time.Second)
			csub, nhCh, err = subFunc()
		}

		logger.Info("Started newHeads proxy subscription")

		dctx.setStatus(delegateStatusOK)
		for dctx.getStatus() == delegateStatusOK {
			select {
			case err = <-csub.Err():
				logger.WithError(err).Info("ETH newHeads delegate subscription error")
				csub.Unsubscribe()

				dctx.setStatus(delegateStatusErr)
				dctx.cancel(err)
			case h := <-nhCh: // notify all delegated subscriptions
				dctx.notify(h)
			}
		}
	}
}

func (client *ethDelegateClient) delegateSubscribeLogs(subId rpc.ID, channel chan *types.Log, filter types.FilterQuery) (*delegateSubscription, error) {
	dCtx := client.getDelegateCtx(logsCtxName)
	if dCtx.getStatus() == delegateStatusErr {
		return nil, errDelegateNotReady
	}

	delegateSub := dCtx.registerDelegateSub(subId, channel, func(item interface{}) bool {
		log, ok := item.(*types.Log)
		return !ok || !matchEthPubSubLogFilter(log, &filter)
	})
	dCtx.run(client.proxySubscribeLogs)

	return delegateSub, nil
}

func (client *ethDelegateClient) proxySubscribeLogs(dctx *delegateContext) {
	subFunc := func() (types.Subscription, chan types.Log, error) {
		logsCh := make(chan types.Log, pubsubChannelBufferSize)
		sub, err := client.Eth.SubscribeFilterLogs(types.FilterQuery{}, logsCh)
		return sub, logsCh, err
	}

	logger := logrus.WithField("nodeURL", client.URL)

	for {
		csub, logsCh, err := subFunc()
		for failures := 0; err != nil; { // resub until suceess
			logger.WithError(err).Info("Logs proxy subscriptions error")

			if failures++; failures%maxProxyDelegateSubFailures == 0 {
				// trigger error for every few failures
				logger.WithField("failures", failures).
					WithError(err).Error("Failed to try too many logs proxy subscriptions")
			}

			time.Sleep(time.Second)
			csub, logsCh, err = subFunc()
		}

		logger.Info("Started logs proxy subscription")

		dctx.setStatus(delegateStatusOK)
		for dctx.getStatus() == delegateStatusOK {
			select {
			case err = <-csub.Err():
				logger.WithError(err).Info("ETH logs delegate subscription error")
				csub.Unsubscribe()

				dctx.setStatus(delegateStatusErr)
				dctx.cancel(err)
			case l := <-logsCh: // notify all delegated subscriptions
				dctx.notify(&l)
			}
		}
	}
}

func matchEthPubSubLogFilter(log *types.Log, filter *types.FilterQuery) bool {
	if len(filter.Addresses) == 0 && len(filter.Topics) == 0 {
		return true
	}

	return matchEthLogFilterAddr(log, filter) && matchEthLogFilterTopic(log, filter)
}

func matchEthLogFilterTopic(log *types.Log, filter *types.FilterQuery) bool {
	find := func(t common.Hash, topics []common.Hash) bool {
		for _, topic := range topics {
			if t == topic {
				return true
			}
		}
		return false
	}

	for i, topics := range filter.Topics {
		if len(topics) == 0 {
			continue
		}

		if len(log.Topics) <= i || !find(log.Topics[i], topics) {
			return false
		}
	}

	return true
}

func matchEthLogFilterAddr(log *types.Log, filter *types.FilterQuery) bool {
	for _, addr := range filter.Addresses {
		if log.Address == addr {
			return true
		}
	}

	return len(filter.Addresses) == 0
}

func isEmptyEthLogFilter(filter types.FilterQuery) bool {
	if len(filter.Addresses) > 0 {
		return false
	}

	for i := range filter.Topics {
		if len(filter.Topics[i]) > 0 {
			return false
		}
	}

	return true
}
