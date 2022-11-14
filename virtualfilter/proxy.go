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
	mu  sync.Mutex
	fid *ProxyFilterID // shared proxy filter ID
}

// loadOrNewProxyFilter creates or loads an universal shared proxy filter
func (pctx *proxyContext) loadOrNewProxyFilter(client *node.Web3goClient) (*ProxyFilterID, bool, error) {
	if pctx.fid != nil { // already created?
		return pctx.fid, false, nil
	}

	fid, err := client.Filter.NewLogFilter(&types.FilterQuery{})
	if err != nil {
		return nil, false, err
	}

	pctx.fid = fid
	return fid, true, nil
}

// delegateContext holds delegate information for the proxy filter
type delegateContext struct {
	client *node.Web3goClient

	termCh chan struct{}  // termination singal channel
	closed bool           // in case channel closed more than once
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

func (dctx *delegateContext) registerGuest(dfid DelegateFilterID) {
	dctx.guestFilters[dfid] = struct{}{}
}

func (dctx *delegateContext) deregisterGuest(dfid DelegateFilterID) {
	delete(dctx.guestFilters, dfid)
}

// close closes the signal channel to terminate the shared proxy filter
func (dctx *delegateContext) close() {
	if !dctx.closed {
		close(dctx.termCh)
		dctx.closed = true
	}
}

// snapshotFilterContext snapshots state for the proxy filter to return new filter context
func (dctx *delegateContext) snapshotFilterContext(crit *types.FilterQuery) *FilterContext {
	return &FilterContext{fid: *dctx.fid, cursor: dctx.cur, crit: crit}
}
