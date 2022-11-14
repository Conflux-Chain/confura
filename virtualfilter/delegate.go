package virtualfilter

import (
	"sync"
	"sync/atomic"

	"github.com/Conflux-Chain/confura/node"
	web3rpc "github.com/openweb3/go-rpc-provider"
	web3Types "github.com/openweb3/web3go/types"
)

// delegateClient establishes singleton proxy filter to the full node
type delegateClient struct {
	*node.Web3goClient

	// used as sync.Once but with support to reset
	mu   sync.Mutex
	done uint32

	// filter ID for the singleton proxy filter
	fid *web3rpc.ID
}

// NewFilter creates singleton proxy filter to full node
func (dc *delegateClient) NewFilter(crit *web3Types.FilterQuery) (*web3rpc.ID, bool, error) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if atomic.LoadUint32(&dc.done) != 0 {
		return dc.fid, false, nil
	}

	// new universal proxy log filter to full node
	fid, err := dc.Filter.NewLogFilter(&web3Types.FilterQuery{})
	if err != nil {
		return nil, false, err
	}

	dc.fid = fid
	atomic.StoreUint32(&dc.done, 1)

	return fid, true, nil
}

func (dc *delegateClient) reset() {
	atomic.StoreUint32(&dc.done, 0)
}

type delegateContext struct {
	mu sync.RWMutex

	termCh chan struct{} // termination singal channel
	oncer  sync.Once     // make sure channel closed only once

	client *delegateClient // delegate client
	fid    *web3rpc.ID     // filter ID
	cur    *FilterCursor   // current filter cursor
}

func newDelegateContext(fid *web3rpc.ID, client *delegateClient) *delegateContext {
	return &delegateContext{
		fid:    fid,
		client: client,
		termCh: make(chan struct{}),
	}
}

func (dctx *delegateContext) close() {
	dctx.oncer.Do(func() {
		close(dctx.termCh)
	})
}

// snapshotFilterContext snapshots current delegate state.
func (dctx *delegateContext) snapshotFilterContext(crit *web3Types.FilterQuery) *FilterContext {
	dctx.mu.Lock()
	defer dctx.mu.Unlock()

	return &FilterContext{fid: *dctx.fid, cursor: dctx.cur, crit: crit}
}
