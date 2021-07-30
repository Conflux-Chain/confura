package rpc

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/rpc"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

type delegateStatus uint32

const (
	// pre-defined delegate status
	delegateStatusInit delegateStatus = iota + 1
	delegateStatusErr
	delegateStatusOK

	// default pubsub channel buffer size
	pubsubChannelBuffer = 1000

	// pre-defined delegate context name
	nhCtxName      = "new_heads"           // for newHeads subscription
	lmEpochCtxName = "latest_mined_epochs" // for latest minted epoch subscription
	lsEpochCtxName = "latest_state_epochs" // for latest state epoch subscription
)

var (
	// errSubscriptionProxyError uniform error returned to the end user when pubsub error.
	errSubscriptionProxyError = errors.New("subscription proxy error")
	// errDelegateNotReady returned when the delegate is not ready for service.
	errDelegateNotReady = errors.New("delegate not ready")

	// delegateClients cache store delegate clients
	delegateClients util.ConcurrentMap // node name => *delegateClient
)

// delegateSubscription is a subscription established through the delegateClient's Subscribe methods.
type delegateSubscription struct {
	dCtx     *delegateContext
	subId    rpc.ID        // rpc subscription ID
	etype    reflect.Type  // channel type
	channel  reflect.Value // channel to send result to
	quitOnce sync.Once     // ensures quit is closed once
	quit     chan struct{} // quit is closed when the subscription exits
	err      chan error    // channel to send/receive delegate error
}

func newDelegateSubscription(dCtx *delegateContext, subId rpc.ID, channel interface{}) *delegateSubscription {
	// check type of channel first
	chanVal := reflect.ValueOf(channel)
	if chanVal.Kind() != reflect.Chan || chanVal.Type().ChanDir()&reflect.SendDir == 0 || chanVal.IsNil() {
		logrus.Fatal("Delegate subscription channel must be a writable channel and not be nil")
	}

	return &delegateSubscription{
		dCtx:    dCtx,
		subId:   subId,
		etype:   chanVal.Type().Elem(),
		channel: chanVal,
		quit:    make(chan struct{}),
		err:     make(chan error, 1),
	}
}

func (sub *delegateSubscription) deliver(result interface{}) bool {
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
	atomic.StoreUint32((*uint32)(&dctx.status), uint32(delegateStatusOK))
}

func (dctx *delegateContext) registerDelegateSub(subId rpc.ID, channel interface{}) *delegateSubscription {
	dctx.lock.Lock()
	defer dctx.lock.Unlock()

	delegateSub := newDelegateSubscription(dctx, subId, channel)
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
	nodeName := util.Url2NodeName(cfx.GetNodeURL())
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
		nhCh := make(chan types.BlockHeader, pubsubChannelBuffer)
		sub, err := client.SubscribeNewHeads(nhCh)
		return sub, nhCh, err
	}

	for {
		csub, nhCh, err := subFunc()
		if err != nil {
			logrus.WithError(err).Error("Failed to do newHeads proxy subscription")
			time.Sleep(time.Second)
			continue
		}

		dctx.setStatus(delegateStatusOK)
		for dctx.getStatus() == delegateStatusOK {
			select {
			case err = <-csub.Err(): // FIXME bug with go sdk which will hang for a few minutes even the internet is cut off
				logrus.WithError(err).Error("Cfx newHeads delegate subscription error")
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
		epochCh := make(chan types.WebsocketEpochResponse, pubsubChannelBuffer)
		sub, err := client.SubscribeEpochs(epochCh, *dctx.epoch)
		return sub, epochCh, err
	}

	for {
		csub, epochCh, err := subFunc()
		if err != nil {
			logrus.WithError(err).Error("Failed to do epochs proxy subscription")
			time.Sleep(time.Second)
			continue
		}

		dctx.setStatus(delegateStatusOK)
		for dctx.getStatus() == delegateStatusOK {
			select {
			case err = <-csub.Err(): // FIXME bug with go sdk which will hang for a few minutes even the internet is cut off
				logrus.WithError(err).Error("Cfx epochs delegate subscription error")
				csub.Unsubscribe()

				dctx.setStatus(delegateStatusErr)
				dctx.cancel(err)
			case h := <-epochCh: // notify all delegated subscriptions
				dctx.notify(&h)
			}
		}
	}
}

// rpcClientFromContext returns the rpc client value stored in ctx, if any.
func rpcClientFromContext(ctx context.Context) (*rpc.Client, bool) {
	client, supported := ctx.Value("client").(*rpc.Client)
	return client, supported
}
