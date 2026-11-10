package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/node"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	ct "github.com/Conflux-Chain/go-conflux-sdk/types"
	rpc "github.com/openweb3/go-rpc-provider"
	wt "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type subscriptionRouter string

func (r subscriptionRouter) Route(node.Group, []byte) string { return string(r) }

type SubscriptionStatusAPI struct{ ready atomic.Bool }

func (*SubscriptionStatusAPI) GetStatus() ct.Status {
	return ct.Status{ChainID: 1, NetworkID: 1, BestHash: ct.Hash("0x" + strings.Repeat("01", 32))}
}

func (*SubscriptionStatusAPI) ChainId() string { return "0x1" }
func (a *SubscriptionStatusAPI) subscription(ctx context.Context) (*rpc.Subscription, error) {
	if !a.ready.Load() {
		return nil, fmt.Errorf("fixture subscription unavailable")
	}
	n, _ := rpc.NotifierFromContext(ctx)
	return n.CreateSubscription(), nil
}
func (a *SubscriptionStatusAPI) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	return a.subscription(ctx)
}
func (a *SubscriptionStatusAPI) Epochs(ctx context.Context, _ *ct.Epoch) (*rpc.Subscription, error) {
	return a.subscription(ctx)
}
func (a *SubscriptionStatusAPI) Logs(ctx context.Context, _ ct.LogFilter) (*rpc.Subscription, error) {
	return a.subscription(ctx)
}

type SubscriptionCreationAPI struct {
	core    *cfxAPI
	eth     *ethAPI
	session *rpcutil.PubSubSession
	created chan bool
}

func (a *SubscriptionCreationAPI) Events(ctx context.Context, kind string) (*rpc.Subscription, error) {
	ctx = rpcutil.ContextWithPubSubSession(ctx, a.session)
	defer func() {
		n, _ := rpc.NotifierFromContext(ctx)
		// Observe the pinned provider's pending resource without changing it.
		a.created <- !reflect.ValueOf(n).Elem().FieldByName("sub").IsNil()
	}()
	switch kind {
	case "core-heads":
		return a.core.NewHeads(ctx)
	case "core-epochs":
		return a.core.Epochs(ctx, nil)
	case "core-logs":
		return a.core.Logs(ctx, ct.LogFilter{})
	case "eth-heads":
		return a.eth.NewHeads(ctx)
	default:
		return a.eth.Logs(ctx, wt.FilterQuery{})
	}
}

func TestSubscriptionCreationFailureDoesNotAllocateProviderSubscription(t *testing.T) {
	for _, kind := range []string{"core-heads", "core-epochs", "core-logs", "eth-heads", "eth-logs"} {
		t.Run(kind, func(t *testing.T) {
			upstream := rpc.NewServer()
			defer upstream.Stop()
			upAPI := &SubscriptionStatusAPI{}
			require.NoError(t, upstream.RegisterName("cfx", upAPI))
			require.NoError(t, upstream.RegisterName("eth", upAPI))
			ws := httptest.NewServer(upstream.WebsocketHandler([]string{"*"}))
			defer ws.Close()
			url := "ws" + strings.TrimPrefix(ws.URL, "http")
			defer delegateClients.Delete(rpcutil.Url2NodeName(url))
			limiter := rpcutil.NewPubSubLimiter(rpcutil.PubSubLimitConfig{MaxSubscriptions: 1})
			session, err := limiter.AcquireConnection("fixture")
			require.NoError(t, err)
			defer session.Close()
			api := &SubscriptionCreationAPI{session: session, created: make(chan bool, 1)}
			if strings.HasPrefix(kind, "core") {
				p := node.NewCfxClientProvider(nil, subscriptionRouter(url))
				api.core = &cfxAPI{provider: p}
				defer func() {
					c, err := p.GetClientByIP(context.Background(), node.GroupCfxWs)
					require.NoError(t, err)
					c.Close()
				}()
			} else {
				p := node.NewEthClientProvider(nil, nil, subscriptionRouter(url))
				api.eth = &ethAPI{provider: p}
				defer func() {
					c, err := p.GetClientByIP(context.Background(), node.GroupEthWs)
					require.NoError(t, err)
					c.Client.Close()
				}()
			}
			server := rpc.NewServer()
			defer server.Stop()
			require.NoError(t, server.RegisterName("fixture", api))
			client := rpc.DialInProc(server)
			defer client.Close()
			for i := 0; i < 4; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := client.Subscribe(ctx, "fixture", make(chan json.RawMessage, 1), "events", kind)
				cancel()
				require.ErrorContains(t, err, "subscription proxy error")
				require.False(t, <-api.created)
				require.Zero(t, limiter.Stats().Subscriptions)
			}
			upAPI.ready.Store(true)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			sub, err := client.Subscribe(ctx, "fixture", make(chan json.RawMessage, 1), "events", kind)
			require.NoError(t, err)
			require.True(t, <-api.created)
			require.Equal(t, 1, limiter.Stats().Subscriptions)
			sub.Unsubscribe()
			require.Eventually(t, func() bool { return limiter.Stats().Subscriptions == 0 }, time.Second, time.Millisecond)
		})
	}
}

type SubscriptionPublicationAPI struct {
	session  *rpcutil.PubSubSession
	delegate *delegateContext
	created  chan bool
}

func (a *SubscriptionPublicationAPI) Events(ctx context.Context, mode string) (*rpc.Subscription, error) {
	ctx = rpcutil.ContextWithPubSubSession(ctx, a.session)
	permit, err := reservePubSubSubscriptionPermit(ctx)
	if err != nil {
		return nil, err
	}
	values := make(chan int, 1)
	dsub, err := a.delegate.registerDelegateSub(nil, rpc.NewID(), values)
	if err != nil {
		releasePubSubSubscriptionPermit(permit)
		return nil, err
	}
	// A notification can arrive before the downstream subscription is published.
	a.delegate.notify(7)
	if mode == "canceled" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	} else if mode == "overflow" {
		a.delegate.notify(8)
	}
	n, _ := rpc.NotifierFromContext(ctx)
	sub, err := newDelegateRPCSubscription(ctx, n, dsub, permit)
	a.created <- !reflect.ValueOf(n).Elem().FieldByName("sub").IsNil()
	if err != nil {
		return nil, err
	}
	go func() {
		defer dsub.unsubscribe()
		defer releasePubSubSubscriptionPermit(permit)
		for {
			select {
			case value := <-values:
				n.Notify(sub.ID, value)
			case <-sub.Err():
				return
			case <-n.Closed():
				return
			}
		}
	}()
	return sub, nil
}

func TestDelegateRPCSubscriptionPublication(t *testing.T) {
	for _, mode := range []string{"ready", "canceled", "overflow"} {
		t.Run(mode, func(t *testing.T) {
			limiter := rpcutil.NewPubSubLimiter(rpcutil.PubSubLimitConfig{MaxSubscriptions: 1})
			session, err := limiter.AcquireConnection("fixture")
			require.NoError(t, err)
			defer session.Close()
			api := &SubscriptionPublicationAPI{session, newDelegateContext(), make(chan bool, 1)}
			server := rpc.NewServer()
			defer server.Stop()
			require.NoError(t, server.RegisterName("fixture", api))
			client := rpc.DialInProc(server)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			values := make(chan int, 1)
			sub, err := client.Subscribe(ctx, "fixture", values, "events", mode)
			if mode == "ready" {
				require.NoError(t, err)
				require.True(t, <-api.created)
				select {
				case value := <-values:
					require.Equal(t, 7, value)
				case <-ctx.Done():
					t.Fatal("lost early notification")
				}
				sub.Unsubscribe()
			} else {
				require.Error(t, err)
				require.False(t, <-api.created)
			}
			require.Eventually(t, func() bool { return limiter.Stats().Subscriptions == 0 }, time.Second, time.Millisecond)
			empty := true
			api.delegate.delegateSubs.Range(func(_, _ interface{}) bool { empty = false; return false })
			require.True(t, empty)
		})
	}
}
