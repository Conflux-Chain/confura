package virtualfilter

import (
	"errors"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura/node"
	web3rpc "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

var (
	// errFilterProxyError uniform error returned to the end user when proxy filter error.
	errFilterProxyError = errors.New("filter proxy error")
)

type proxyContext struct {
	// shared proxy filter ID
	fid ProxyFilterID

	// current polling cursor
	cur FilterCursor

	// delegate filters
	delegates map[DelegateFilterID]*FilterContext
}

func newProxyContext(fid ProxyFilterID) *proxyContext {
	return &proxyContext{
		fid:       fid,
		delegates: make(map[web3rpc.ID]*FilterContext),
	}
}

type proxyStub struct {
	mu     sync.Mutex
	client *node.Web3goClient
	pctx   *proxyContext
}

func newProxyStub(client *node.Web3goClient) *proxyStub {
	return &proxyStub{client: client}
}

func (p *proxyStub) newFilter(crit *types.FilterQuery) (*web3rpc.ID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// establish shared proxy filter to the full node
	if err := p.establish(); err != nil {
		return nil, err
	}

	// new virtual delegate filter
	dfid := web3rpc.NewID()

	// snapshot shared filter cursor
	p.pctx.delegates[dfid] = &FilterContext{
		cursor: p.pctx.cur, crit: crit,
	}

	return &dfid, nil
}

func (p *proxyStub) uninstallFilter(id *DelegateFilterID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pctx == nil {
		return false
	}

	if _, ok := p.pctx.delegates[*id]; !ok {
		return false
	}

	delete(p.pctx.delegates, *id)
	return true
}

// poll instantly polling filter changes from full node
func (p *proxyStub) poll() {
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	lastPollingTime := time.Now()

	for range ticker.C {
		if p.gc() { // garbage collected?
			return
		}

		fchanges, err := p.client.Filter.GetFilterChanges(p.pctx.fid)
		if err == nil {
			lastPollingTime = time.Now() // update last polling time
			p.merge(fchanges)
			continue
		}

		// Proxy filter not found by full node? this may be due to full node reboot.
		if IsFilterNotFoundError(err) {
			p.close()
			return
		}

		if dur := time.Since(lastPollingTime); dur > maxPollingDelayDuration { // too many times delayed?
			logrus.WithFields(logrus.Fields{
				"proxyFilterID": p.pctx.fid, "delayedDuration": dur,
			}).WithError(err).Error("Filter proxy failed to poll filter changes after too many delays")

			p.close()
			return
		}
	}
}

func (p *proxyStub) merge(changes *types.FilterChanges) {
	// TODO: merge filter changes to db && cache store
}

// close closes the proxy so that new shared proxy filter can be recreated
func (p *proxyStub) close(lockfree ...bool) {
	if len(lockfree) == 0 || !lockfree[0] {
		p.mu.Lock()
		defer p.mu.Unlock()
	}

	if p.pctx != nil {
		// uninstall the proxy filter with error ignored
		p.client.Filter.UninstallFilter(p.pctx.fid)

		// reset proxy context
		p.pctx = nil
	}
}

// gc closes the proxy if not under use by any delegate filter
func (p *proxyStub) gc() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pctx != nil && len(p.pctx.delegates) == 0 {
		p.close(true)
		return true
	}

	return false
}

// establishes a new shared filter to full node if necessary
func (p *proxyStub) establish() error {
	if p.pctx != nil { // already established
		return nil
	}

	// establishes a shared universal proxy filter to full node
	fid, err := p.client.Filter.NewLogFilter(&types.FilterQuery{})
	if err != nil {
		return err
	}

	p.pctx = newProxyContext(*fid)

	// start polling from full node instantly
	go p.poll()

	return nil
}
