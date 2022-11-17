package virtualfilter

import (
	"errors"
	"sync"

	"github.com/Conflux-Chain/confura/node"
)

var (
	// errFilterProxyError uniform error returned to the end user when proxy filter error.
	errFilterProxyError = errors.New("filter proxy error")
)

// proxyContext holds filtering state for proxy filter
type proxyContext struct {
	client *node.Web3goClient
	mu     sync.Mutex

	fid *ProxyFilterID // proxy filter ID
	cur *FilterCursor  // current cursor

	// filter context for the guest delegate filters
	delegateFilters map[DelegateFilterID]*FilterContext
}

func newProxyContext(client *node.Web3goClient) *proxyContext {
	return &proxyContext{
		client:          client,
		delegateFilters: make(map[DelegateFilterID]*FilterContext),
	}
}
