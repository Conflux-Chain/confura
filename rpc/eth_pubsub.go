package rpc

import (
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/util"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
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

func (client *ethDelegateClient) delegateSubscribeNewHeads(
	subId rpc.ID, channel chan *types.Header) (*delegateSubscription, error) {
	dctx := client.getDelegateCtx(nhCtxName)
	if dctx.getStatus() == delegateStatusErr {
		return nil, errDelegateNotReady
	}

	return dctx.registerDelegateSub(client.proxySubscribeNewHeads, subId, channel)
}

func (client *ethDelegateClient) proxySubscribeNewHeads(dctx *delegateContext) error {
	nhCh := make(chan *types.Header, pubsubChannelBufferSize)
	csub, err := client.Eth.SubscribeNewHead(nhCh)
	if err != nil {
		logrus.WithField("nodeURL", client.URL).
			WithError(err).
			Info("ETH Pub/Sub NewHead proxy subscription conn error")
		return err
	}

	go func() { // run subscription loop
		dctx.setStatus(delegateStatusOK)
		defer dctx.setStatus(delegateStatusInit)

		for dctx.getStatus() == delegateStatusOK {
			select {
			case err = <-csub.Err():
				logrus.WithField("nodeURL", client.URL).
					WithError(err).
					Info("ETH Pub/Sub NewHeads proxy subscription delegate error")

				dctx.setStatus(delegateStatusErr)
				csub.Unsubscribe()
				dctx.cancel(err)
			case h := <-nhCh: // notify all delegated subscriptions
				dctx.notify(h)
			}
		}
	}()

	logrus.WithField("nodeURL", client.URL).
		Info("Eth Pub/Sub NewHeads proxy subscription run loop started")
	return nil
}

func (client *ethDelegateClient) delegateSubscribeLogs(
	subId rpc.ID, channel chan *types.Log, filter types.FilterQuery) (*delegateSubscription, error) {

	dCtx := client.getDelegateCtx(logsCtxName)
	if dCtx.getStatus() == delegateStatusErr {
		return nil, errDelegateNotReady
	}

	return dCtx.registerDelegateSub(client.proxySubscribeLogs, subId, channel, func(item interface{}) bool {
		log, ok := item.(*types.Log)
		return !ok || !matchEthPubSubLogFilter(log, &filter)
	})
}

func (client *ethDelegateClient) proxySubscribeLogs(dctx *delegateContext) error {
	logsCh := make(chan types.Log, pubsubChannelBufferSize)
	csub, err := client.Eth.SubscribeFilterLogs(types.FilterQuery{}, logsCh)
	if err != nil {
		logrus.WithField("nodeURL", client.URL).
			WithError(err).
			Info("ETH Pub/Sub Logs proxy subscription conn error")
		return err
	}

	go func() { // run subscription loop
		dctx.setStatus(delegateStatusOK)
		defer dctx.setStatus(delegateStatusInit)

		for dctx.getStatus() == delegateStatusOK {
			select {
			case err = <-csub.Err():
				logrus.WithField("nodeURL", client.URL).
					WithError(err).
					Info("ETH Pub/Sub Logs delegate subscription delegate error")

				dctx.setStatus(delegateStatusErr)
				csub.Unsubscribe()
				dctx.cancel(err)
			case l := <-logsCh: // notify all delegated subscriptions
				dctx.notify(&l)
			}
		}
	}()

	logrus.WithField("nodeURL", client.URL).
		Info("Eth Pub/Sub Logs proxy subscription run loop started")
	return nil
}

func matchEthPubSubLogFilter(log *types.Log, filter *types.FilterQuery) bool {
	if len(filter.Addresses) == 0 && len(filter.Topics) == 0 {
		return true
	}

	return util.IncludeEthLogAddrs(log, filter.Addresses) && util.MatchEthLogTopics(log, filter.Topics)
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
