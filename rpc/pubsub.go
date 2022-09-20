package rpc

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
	"github.com/scroll-tech/rpc-gateway/util"
	rpcutil "github.com/scroll-tech/rpc-gateway/util/rpc"
	"github.com/sirupsen/logrus"
)

type delegateStatus uint32

const (
	// pre-defined delegate status
	delegateStatusInit delegateStatus = iota + 1
	delegateStatusErr
	delegateStatusOK

	// default pubsub channel buffer size
	pubsubChannelBufferSize = 2000

	// pre-defined delegate context name
	nhCtxName      = "new_heads"           // for newHeads subscription
	lmEpochCtxName = "latest_mined_epochs" // for latest minted epoch subscription
	lsEpochCtxName = "latest_state_epochs" // for latest state epoch subscription
	logsCtxName    = "logs"                // for logs subscription

	// max proxy delegate subscription failures to trigger error
	maxProxyDelegateSubFailures = 3
)

var (
	// errSubscriptionProxyError uniform error returned to the end user when pubsub error.
	errSubscriptionProxyError = errors.New("subscription proxy error")
	// errDelegateNotReady returned when the delegate is not ready for service.
	errDelegateNotReady = errors.New("delegate not ready")

	// delegateClients cache store delegate clients
	delegateClients util.ConcurrentMap // node name => *delegateClient
)

type delegateSubFilter func(item interface{}) bool // result filter for delegate subscription

// delegateSubscription is a subscription established through the delegateClient's `Subscribe` methods.
type delegateSubscription struct {
	dCtx     *delegateContext
	subId    rpc.ID              // rpc subscription ID
	etype    reflect.Type        // channel type
	channel  reflect.Value       // channel to send result to
	quitOnce sync.Once           // ensures quit is closed once
	quit     chan struct{}       // quit is closed when the subscription exits
	err      chan error          // channel to send/receive delegate error
	filters  []delegateSubFilter // blacklist filter chain
}

func newDelegateSubscription(dCtx *delegateContext, subId rpc.ID, channel interface{}, filters ...delegateSubFilter) *delegateSubscription {
	// check type of channel first
	chanVal := reflect.ValueOf(channel)
	if chanVal.Kind() != reflect.Chan || chanVal.Type().ChanDir()&reflect.SendDir == 0 || chanVal.IsNil() {
		panic("Delegate subscription channel must be a writable channel and not be nil")
	}

	return &delegateSubscription{
		dCtx:    dCtx,
		subId:   subId,
		etype:   chanVal.Type().Elem(),
		channel: chanVal,
		quit:    make(chan struct{}),
		err:     make(chan error, 1),
		filters: filters,
	}
}

func (sub *delegateSubscription) deliver(result interface{}) bool {
	// filter result before deliver
	for _, blacklist := range sub.filters {
		if blacklist(result) {
			logrus.WithField("result", result).Debug("Blacklisted to deliver from delegate subscription")
			return false
		}
	}

	cases := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sub.quit)},
		{Dir: reflect.SelectSend, Chan: sub.channel, Send: reflect.ValueOf(result)},
		{Dir: reflect.SelectDefault},
	}

	switch index, _, _ := reflect.Select(cases); index {
	case 0: // <-sub.quit:
		return false
	case 1: // sub.channel<-
		return true
	case 2: // never blocking for subscription queue overflow
		sub.err <- rpc.ErrSubscriptionQueueOverflow
		return false
	}

	return false
}

// unsubscribe the notification and closes the error channel.
// It can safely be called more than once.
func (sub *delegateSubscription) unsubscribe() {
	sub.quitOnce.Do(func() {
		close(sub.quit)
		close(sub.err)

		// deregister delegate subscriptions
		sub.dCtx.deregisterDelegateSub(sub.subId)
	})
}

// delegateContext delegate context used for each delegation type
type delegateContext struct {
	oncer        sync.Once
	lock         sync.RWMutex
	status       delegateStatus
	delegateSubs util.ConcurrentMap // client subscription ID => *delegateSubscription

	epoch *types.Epoch // epochs subscription type
}

// functional options to set delegateContext
type delegateCtxOption func(ctx *delegateContext)

func newDelegateContext(options ...delegateCtxOption) *delegateContext {
	ctx := &delegateContext{status: delegateStatusInit}
	for i := 0; i < len(options); i++ {
		options[i](ctx)
	}

	return ctx
}

func withEpoch(epoch *types.Epoch) delegateCtxOption {
	return func(ctx *delegateContext) {
		ctx.epoch = epoch
	}
}

func (dctx *delegateContext) getStatus() delegateStatus {
	return delegateStatus(atomic.LoadUint32((*uint32)(&dctx.status)))
}

func (dctx *delegateContext) setStatus(status delegateStatus) {
	atomic.StoreUint32((*uint32)(&dctx.status), uint32(status))
}

func (dctx *delegateContext) registerDelegateSub(subId rpc.ID, channel interface{}, filters ...delegateSubFilter) *delegateSubscription {
	dctx.lock.Lock()
	defer dctx.lock.Unlock()

	delegateSub := newDelegateSubscription(dctx, subId, channel, filters...)
	dctx.delegateSubs.Store(subId, delegateSub)

	return delegateSub
}

func (dctx *delegateContext) deregisterDelegateSub(subId rpc.ID) *delegateSubscription {
	dctx.lock.Lock()
	defer dctx.lock.Unlock()

	if dsub, loaded := dctx.delegateSubs.LoadAndDelete(subId); loaded {
		return dsub.(*delegateSubscription)
	}

	return nil
}

// run pubsub delegate subscription once
func (dctx *delegateContext) run(delegateFunc func(dctx *delegateContext)) {
	dctx.oncer.Do(func() {
		go delegateFunc(dctx)
	})
}

// cancel all delegated subscriptions
func (dctx *delegateContext) cancel(err error) {
	dctx.lock.Lock()
	defer dctx.lock.Unlock()

	if err == nil {
		err = rpc.ErrClientQuit
	}

	dctx.delegateSubs.Range(func(key, value interface{}) bool {
		dsub := value.(*delegateSubscription)
		dsub.err <- err

		dctx.delegateSubs.Delete(key)
		return true
	})
}

// notify all delegated subscriptions for new result
func (dctx *delegateContext) notify(result interface{}) {
	dctx.lock.RLock()
	defer dctx.lock.RUnlock()

	dctx.delegateSubs.Range(func(key, value interface{}) bool {
		dsub := value.(*delegateSubscription)
		dsub.deliver(result)

		return true
	})
}

// delegateClient client delegated for pubsub subscription
type delegateClient struct {
	sdk.ClientOperator

	delegateContexts util.ConcurrentMap // context name => *delegateContext
}

func getOrNewDelegateClient(cfx sdk.ClientOperator) *delegateClient {
	nodeName := rpcutil.Url2NodeName(cfx.GetNodeURL())
	client, _ := delegateClients.LoadOrStore(nodeName, &delegateClient{ClientOperator: cfx})
	return client.(*delegateClient)
}

func (client *delegateClient) getDelegateCtx(ctxName string) *delegateContext {
	dctx, _ := client.delegateContexts.LoadOrStore(ctxName, newDelegateContext())
	return dctx.(*delegateContext)
}

func (client *delegateClient) getDelegateCtxWithEpoch(ctxName string, epoch *types.Epoch) *delegateContext {
	dctx, _ := client.delegateContexts.LoadOrStore(ctxName, newDelegateContext(withEpoch(epoch)))
	return dctx.(*delegateContext)
}

func (client *delegateClient) delegateSubscribeNewHeads(subId rpc.ID, channel chan *types.BlockHeader) (*delegateSubscription, error) {
	dCtx := client.getDelegateCtx(nhCtxName)
	if dCtx.getStatus() == delegateStatusErr {
		return nil, errDelegateNotReady
	}

	delegateSub := dCtx.registerDelegateSub(subId, channel)
	dCtx.run(client.proxySubscribeNewHeads)

	return delegateSub, nil
}

func (client *delegateClient) proxySubscribeNewHeads(dctx *delegateContext) {
	subFunc := func() (*rpc.ClientSubscription, chan types.BlockHeader, error) {
		nhCh := make(chan types.BlockHeader, pubsubChannelBufferSize)
		sub, err := client.SubscribeNewHeads(nhCh)
		return sub, nhCh, err
	}

	logger := logrus.WithField("nodeURL", client.GetNodeURL())

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
				logger.WithError(err).Info("Cfx newHeads delegate subscription error")
				csub.Unsubscribe()

				dctx.setStatus(delegateStatusErr)
				dctx.cancel(err)
			case h := <-nhCh: // notify all delegated subscriptions
				dctx.notify(&h)
			}
		}
	}
}

func (client *delegateClient) delegateSubscribeEpochs(subId rpc.ID, channel chan *types.WebsocketEpochResponse, subscriptionEpochType ...types.Epoch) (*delegateSubscription, error) {
	subEpochType, dctxName := types.EpochLatestMined, lmEpochCtxName
	if len(subscriptionEpochType) > 0 && subscriptionEpochType[0].Equals(types.EpochLatestState) {
		subEpochType, dctxName = types.EpochLatestState, lsEpochCtxName
	}

	dCtx := client.getDelegateCtxWithEpoch(dctxName, subEpochType)
	if dCtx.getStatus() == delegateStatusErr {
		return nil, errDelegateNotReady
	}

	delegateSub := dCtx.registerDelegateSub(subId, channel)
	dCtx.run(client.proxySubscribeEpochs)

	return delegateSub, nil
}

func (client *delegateClient) proxySubscribeEpochs(dctx *delegateContext) {
	subFunc := func() (*rpc.ClientSubscription, chan types.WebsocketEpochResponse, error) {
		epochCh := make(chan types.WebsocketEpochResponse, pubsubChannelBufferSize)
		sub, err := client.SubscribeEpochs(epochCh, *dctx.epoch)
		return sub, epochCh, err
	}

	logger := logrus.WithFields(logrus.Fields{
		"nodeURL":      client.GetNodeURL(),
		"subEpochType": dctx.epoch,
	})

	for {
		csub, epochCh, err := subFunc()
		for failures := 0; err != nil; { // resub until suceess
			logger.WithError(err).Info("Epochs proxy subscriptions error")

			if failures++; failures%maxProxyDelegateSubFailures == 0 {
				// trigger error for every few failures
				logger.WithField("failures", failures).
					WithError(err).
					Error("Failed to try too many epochs proxy subscriptions")
			}

			time.Sleep(time.Second)
			csub, epochCh, err = subFunc()
		}

		logger.Info("Started epochs proxy subscription")

		dctx.setStatus(delegateStatusOK)
		for dctx.getStatus() == delegateStatusOK {
			select {
			case err = <-csub.Err():
				logger.WithError(err).Info("Cfx epochs delegate subscription error")
				csub.Unsubscribe()

				dctx.setStatus(delegateStatusErr)
				dctx.cancel(err)
			case e := <-epochCh: // notify all delegated subscriptions
				dctx.notify(&e)
			}
		}
	}
}

func (client *delegateClient) delegateSubscribeLogs(subId rpc.ID, channel chan *types.SubscriptionLog, filter types.LogFilter) (*delegateSubscription, error) {
	dCtx := client.getDelegateCtx(logsCtxName)
	if dCtx.getStatus() == delegateStatusErr {
		return nil, errDelegateNotReady
	}

	delegateSub := dCtx.registerDelegateSub(subId, channel, func(item interface{}) bool {
		log, ok := item.(*types.SubscriptionLog)
		return !ok || !matchPubSubLogFilter(log, &filter)
	})
	dCtx.run(client.proxySubscribeLogs)

	return delegateSub, nil
}

func (client *delegateClient) proxySubscribeLogs(dctx *delegateContext) {
	subFunc := func() (*rpc.ClientSubscription, chan types.SubscriptionLog, error) {
		logsCh := make(chan types.SubscriptionLog, pubsubChannelBufferSize)
		sub, err := client.SubscribeLogs(logsCh, types.LogFilter{})
		return sub, logsCh, err
	}

	logger := logrus.WithField("nodeURL", client.GetNodeURL())

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
				logger.WithError(err).Info("Cfx logs delegate subscription error")
				csub.Unsubscribe()

				dctx.setStatus(delegateStatusErr)
				dctx.cancel(err)
			case l := <-logsCh: // notify all delegated subscriptions
				dctx.notify(&l)
			}
		}
	}
}

func matchPubSubLogFilter(log *types.SubscriptionLog, filter *types.LogFilter) bool {
	if (len(filter.Address) == 0 && len(filter.Topics) == 0) || log.IsRevertLog() {
		return true
	}

	return matchLogFilterAddr(log, filter) && matchLogFilterTopic(log, filter)
}

func matchLogFilterTopic(log *types.SubscriptionLog, filter *types.LogFilter) bool {
	find := func(t types.Hash, topics []types.Hash) bool {
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

func matchLogFilterAddr(log *types.SubscriptionLog, filter *types.LogFilter) bool {
	for _, addr := range filter.Address {
		if log.Address.Equals(&addr) {
			return true
		}
	}

	return len(filter.Address) == 0
}

func isEmptyLogFilter(filter types.LogFilter) bool {
	if len(filter.Address) > 0 {
		return false
	}

	for i := range filter.Topics {
		if len(filter.Topics[i]) > 0 {
			return false
		}
	}

	return true
}

// rpcClientFromContext returns the rpc client value stored in ctx, if any.
func rpcClientFromContext(ctx context.Context) (*rpc.Client, bool) {
	client, supported := ctx.Value("client").(*rpc.Client)
	return client, supported
}
