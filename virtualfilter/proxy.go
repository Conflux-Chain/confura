package virtualfilter

import (
	"errors"
	"sync"

	"github.com/Conflux-Chain/confura/node"
	"github.com/openweb3/web3go/types"
)

var (
	// errFilterProxyError uniform error returned to the end user when proxy filter error.
	errFilterProxyError = errors.New("filter proxy error")
)

// proxyContext context for shared proxy filter to the full node
type proxyContext struct {
	client *node.Web3goClient
	fid    *ProxyFilterID
}

// proxyFilterManager manages to create proxy filter to full node.
// To reduce workload, there would be only one valid shared proxy filter
// for each full node.
type proxyFilterManager struct {
	mu           sync.Mutex
	proxyFilters map[string]*proxyContext // node name => proxy context
}

func newProxyFilterManager() *proxyFilterManager {
	return &proxyFilterManager{
		proxyFilters: make(map[string]*proxyContext),
	}
}

func (pfm *proxyFilterManager) loadOrNewProxyFilter(
	client *node.Web3goClient, onProxyFilter func(pctx *proxyContext, loaded bool) error) error {

	pfm.mu.Lock()
	defer pfm.mu.Unlock()

	nodeName := client.NodeName()
	if pctx, ok := pfm.proxyFilters[nodeName]; ok {
		return onProxyFilter(pctx, true)
	}

	fid, err := client.Filter.NewLogFilter(&types.FilterQuery{})
	if err != nil {
		return err
	}

	pctx := &proxyContext{client: client, fid: fid}
	pfm.proxyFilters[nodeName] = pctx

	return onProxyFilter(pctx, false)
}

func (pfm *proxyFilterManager) removeProxyFilter(client *node.Web3goClient) {
	pfm.mu.Lock()
	defer pfm.mu.Unlock()

	delete(pfm.proxyFilters, client.NodeName())
}

// delegateContext holds delegate information for the proxy filter
type delegateContext struct {
	client *node.Web3goClient

	termCh chan struct{}  // termination singal channel
	fid    *ProxyFilterID // shared proxy filter ID
	cur    *FilterCursor  // current visiting cursor

	// hashset for the delegate filters which make use of the
	// shared proxy filter
	guestFilters map[DelegateFilterID]struct{}
}

func newDelegateContext(fid *ProxyFilterID, client *node.Web3goClient) *delegateContext {
	return &delegateContext{
		fid:          fid,
		client:       client,
		guestFilters: make(map[DelegateFilterID]struct{}),
		termCh:       make(chan struct{}),
	}
}

func (dctx *delegateContext) registerGuest(dfid *DelegateFilterID) {
	dctx.guestFilters[*dfid] = struct{}{}
}

func (dctx *delegateContext) deregisterGuest(dfid *DelegateFilterID) {
	delete(dctx.guestFilters, *dfid)
}

// close closes the signal channel to terminate the shared proxy filter
func (dctx *delegateContext) close() {
	if dctx.termCh != nil {
		close(dctx.termCh)

		// ensure channel closed only once
		dctx.termCh = nil
	}
}

// snapshotFilterContext snapshots state for the proxy filter to return new filter context
func (dctx *delegateContext) snapshotFilterContext(crit *types.FilterQuery) *FilterContext {
	return &FilterContext{fid: *dctx.fid, cursor: dctx.cur, crit: crit}
}
