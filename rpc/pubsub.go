package rpc

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/Conflux-Chain/confura/util"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
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
)

var (
	// errSubscriptionProxyError uniform error returned to the end user when pubsub error.
	errSubscriptionProxyError = errors.New("subscription proxy error")
	// errDelegateNotReady returned when the delegate is not ready for service.
	errDelegateNotReady = errors.New("delegate not ready")

	// delegateClients cache store delegate clients
	delegateClients util.ConcurrentMap // node name => *delegateClient
)

type delegateSubFilter func(item any) bool // result filter for delegate subscription

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

func newDelegateSubscription(
	dCtx *delegateContext, subId rpc.ID, channel any, filters ...delegateSubFilter) *delegateSubscription {

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

func (sub *delegateSubscription) deliver(result any) bool {
	// filter result before deliver
	for _, blacklist := range sub.filters {
		if blacklist(result) {
			logrus.WithField("result", result).
				Debug("Blacklisted to deliver from delegate subscription")
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
		// deregister delegate subscriptions
		sub.dCtx.deregisterDelegateSub(sub.subId)

		close(sub.quit)
		close(sub.err)
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

func (dctx *delegateContext) registerDelegateSub(
	pubsubRunLoop func(dctx *delegateContext) error,
	subId rpc.ID, channel any, filters ...delegateSubFilter,
) (*delegateSubscription, error) {
	dctx.lock.Lock()
	defer dctx.lock.Unlock()

	var err error
	if pubsubRunLoop != nil { // start delegate pubsub run loop once
		dctx.oncer.Do(func() {
			err = pubsubRunLoop(dctx)
		})
	}

	if err != nil {
		dctx.oncer = sync.Once{}
		return nil, err
	}

	delegateSub := newDelegateSubscription(dctx, subId, channel, filters...)
	dctx.delegateSubs.Store(subId, delegateSub)

	return delegateSub, nil
}

func (dctx *delegateContext) deregisterDelegateSub(subId rpc.ID) *delegateSubscription {
	dctx.lock.Lock()
	defer dctx.lock.Unlock()

	if dsub, loaded := dctx.delegateSubs.LoadAndDelete(subId); loaded {
		return dsub.(*delegateSubscription)
	}

	return nil
}

// cancel all delegated subscriptions
func (dctx *delegateContext) cancel(err error) {
	dctx.lock.Lock()
	defer dctx.lock.Unlock()

	if err == nil {
		err = rpc.ErrClientQuit
	}

	dctx.delegateSubs.Range(func(key, value any) bool {
		dsub := value.(*delegateSubscription)
		dsub.err <- err

		dctx.delegateSubs.Delete(key)
		return true
	})

	dctx.oncer = sync.Once{}
}

// notify all delegated subscriptions for new result
func (dctx *delegateContext) notify(result any) {
	dctx.lock.RLock()
	defer dctx.lock.RUnlock()

	dctx.delegateSubs.Range(func(key, value any) bool {
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

func (client *delegateClient) delegateSubscribeNewHeads(
	subId rpc.ID, channel chan *types.BlockHeader) (*delegateSubscription, error) {

	dCtx := client.getDelegateCtx(nhCtxName)
	if dCtx.getStatus() == delegateStatusErr {
		return nil, errDelegateNotReady
	}

	return dCtx.registerDelegateSub(client.proxySubscribeNewHeads, subId, channel)
}

func (client *delegateClient) proxySubscribeNewHeads(dctx *delegateContext) error {
	nhCh := make(chan types.BlockHeader, pubsubChannelBufferSize)
	csub, err := client.SubscribeNewHeads(nhCh)
	if err != nil {
		logrus.WithField("nodeURL", client.GetNodeURL()).
			WithError(err).
			Info("CFX Pub/Sub NewHead proxy subscription conn error")
		return err
	}

	go func() { // run subscription loop
		dctx.setStatus(delegateStatusOK)
		defer dctx.setStatus(delegateStatusInit)

		for dctx.getStatus() == delegateStatusOK {
			select {
			case err = <-csub.Err():
				logrus.WithField("nodeURL", client.GetNodeURL()).
					WithError(err).
					Info("CFX Pub/Sub NewHeads subscription delegate error")

				dctx.setStatus(delegateStatusErr)
				csub.Unsubscribe()
				dctx.cancel(err)
			case h := <-nhCh: // notify all delegated subscriptions
				dctx.notify(&h)
			}
		}
	}()

	logrus.WithField("nodeURL", client.GetNodeURL()).
		Info("CFX Pub/Sub NewHead proxy subscription run loop started")
	return nil
}

func (client *delegateClient) delegateSubscribeEpochs(
	subId rpc.ID, channel chan *types.WebsocketEpochResponse, subscriptionEpochType ...types.Epoch,
) (*delegateSubscription, error) {
	subEpochType, dctxName := types.EpochLatestMined, lmEpochCtxName
	if len(subscriptionEpochType) > 0 && subscriptionEpochType[0].Equals(types.EpochLatestState) {
		subEpochType, dctxName = types.EpochLatestState, lsEpochCtxName
	}

	dCtx := client.getDelegateCtxWithEpoch(dctxName, subEpochType)
	if dCtx.getStatus() == delegateStatusErr {
		return nil, errDelegateNotReady
	}

	return dCtx.registerDelegateSub(client.proxySubscribeEpochs, subId, channel)
}

func (client *delegateClient) proxySubscribeEpochs(dctx *delegateContext) error {
	logger := logrus.WithFields(logrus.Fields{
		"nodeURL":      client.GetNodeURL(),
		"subEpochType": dctx.epoch,
	})

	epochCh := make(chan types.WebsocketEpochResponse, pubsubChannelBufferSize)
	csub, err := client.SubscribeEpochs(epochCh, *dctx.epoch)
	if err != nil {
		logger.WithError(err).Info("CFX Pub/Sub Epochs proxy subscription conn error")
		return err
	}

	go func() { // run subscription loop
		dctx.setStatus(delegateStatusOK)
		defer dctx.setStatus(delegateStatusInit)

		for dctx.getStatus() == delegateStatusOK {
			select {
			case err = <-csub.Err():
				logger.WithError(err).Info("CFX Pub/Sub Epochs proxy subscription delegate error")

				dctx.setStatus(delegateStatusErr)
				csub.Unsubscribe()
				dctx.cancel(err)
			case e := <-epochCh: // notify all delegated subscriptions
				dctx.notify(&e)
			}
		}
	}()

	logrus.WithField("nodeURL", client.GetNodeURL()).
		Info("CFX Pub/Sub Epochs proxy subscription run loop started")
	return nil
}

func (client *delegateClient) delegateSubscribeLogs(
	subId rpc.ID, channel chan *types.SubscriptionLog, filter types.LogFilter) (*delegateSubscription, error) {

	dCtx := client.getDelegateCtx(logsCtxName)
	if dCtx.getStatus() == delegateStatusErr {
		return nil, errDelegateNotReady
	}

	return dCtx.registerDelegateSub(client.proxySubscribeLogs, subId, channel, func(item any) bool {
		log, ok := item.(*types.SubscriptionLog)
		return !ok || !matchPubSubLogFilter(log, &filter)
	})
}

func (client *delegateClient) proxySubscribeLogs(dctx *delegateContext) error {
	logsCh := make(chan types.SubscriptionLog, pubsubChannelBufferSize)
	csub, err := client.SubscribeLogs(logsCh, types.LogFilter{})
	if err != nil {
		logrus.WithField("nodeURL", client.GetNodeURL()).
			WithError(err).
			Info("CFX Pub/Sub Logs subscription conn error")
		return err
	}

	go func() { // run subscription loop
		dctx.setStatus(delegateStatusOK)
		defer dctx.setStatus(delegateStatusInit)

		for dctx.getStatus() == delegateStatusOK {
			select {
			case err = <-csub.Err():
				logrus.WithField("nodeURL", client.GetNodeURL()).
					WithError(err).
					Info("CFX Pub/Sub Logs proxy subscription delegate error")

				dctx.setStatus(delegateStatusErr)
				csub.Unsubscribe()
				dctx.cancel(err)
			case l := <-logsCh: // notify all delegated subscriptions
				dctx.notify(&l)
			}
		}
	}()

	logrus.WithField("nodeURL", client.GetNodeURL()).
		Info("CFX Pub/Sub Logs proxy subscription run loop started")
	return nil
}

func matchPubSubLogFilter(log *types.SubscriptionLog, filter *types.LogFilter) bool {
	if (len(filter.Address) == 0 && len(filter.Topics) == 0) || log.IsRevertLog() {
		return true
	}

	return util.IncludeCfxLogAddrs(log.Log, filter.Address) && util.MatchCfxLogTopics(log.Log, filter.Topics)
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
