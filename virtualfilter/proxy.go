package virtualfilter

import (
	"sync"
	"sync/atomic"
	"time"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/util/metrics"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	web3rpc "github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	nilProxyContext = newProxyContext(nilRpcId, 0)

	errProxyAlreadyClosed = errors.New("proxy closed already")
)

// filterProxyError uniform error returned to the end user
func filterProxyError(err error) error {
	return errors.WithMessage(err, "filter proxy error")
}

type proxyObserver interface {
	onEstablished(proxy *proxyStub)
	onClosed(proxy *proxyStub)
	onPolled(proxy *proxyStub, changes *types.FilterChanges) error
}

type proxyContext struct {
	// shared proxy filter ID
	fid ProxyFilterID

	// delegate filters
	delegates map[DelegateFilterID]*FilterContext

	// simulated filter blockchain
	chain *FilterChain
}

func newProxyContext(fid ProxyFilterID, maxFullFilterBlocks int) proxyContext {
	return proxyContext{
		fid:       fid,
		delegates: make(map[web3rpc.ID]*FilterContext),
		chain:     NewFilterChain(maxFullFilterBlocks),
	}
}

type proxyStub struct {
	proxyContext
	shutdownCtx cmdutil.GracefulShutdownContext
	mu          sync.Mutex
	obs         proxyObserver
	cfg         *Config
	client      *node.Web3goClient
	quitflag    int32
}

func newProxyStub(
	shutdownCtx cmdutil.GracefulShutdownContext, cfg *Config, obs proxyObserver, client *node.Web3goClient) *proxyStub {

	return &proxyStub{
		proxyContext: nilProxyContext, shutdownCtx: shutdownCtx, obs: obs, cfg: cfg, client: client,
	}
}

func (p *proxyStub) newFilter(crit *types.FilterQuery) (*web3rpc.ID, error) {
	if atomic.LoadInt32(&p.quitflag) != 0 { // proxy already closed?
		return nil, errProxyAlreadyClosed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// establish shared proxy filter to the full node
	if err := p.establish(); err != nil {
		return nil, err
	}

	// new virtual delegate filter
	dfid := web3rpc.NewID()

	// snapshot the tail cursor of the filter chain
	p.delegates[dfid] = &FilterContext{
		crit: crit, cursor: p.chain.SnapshotCurrentCursor(),
	}

	return &dfid, nil
}

func (p *proxyStub) uninstallFilter(id DelegateFilterID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.delegates[id]; !ok {
		return false
	}

	delete(p.delegates, id)
	return true
}

func (p *proxyStub) getFilterChanges(id DelegateFilterID) ([]FilterBlock, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	fctx, ok := p.delegates[id]
	if !ok {
		return nil, errFilterNotFound
	}

	metricTimer := metrics.Registry.VirtualFilter.QueryFilterChanges(p.client.NodeName(), "memory")
	defer metricTimer.Update()

	var res []FilterBlock

	err := p.chain.Traverse(fctx.cursor, func(node *FilterNode, forkPoint bool) bool {
		if node == nil { // skip nil node
			return true
		}

		if fctx.cursor == nilFilterCursor { // full traversal for nil cursor
			res = append(res, *node.FilterBlock)
			return true
		}

		// otherwise for non-nil cursor

		if forkPoint { // ignore fork point
			return true
		}

		// skip node at the filter cursor, whose block is not re-organized
		if fctx.cursor == node.FilterCursor && !node.reorg {
			return true
		}

		res = append(res, *node.FilterBlock)
		return true
	})

	if err != nil {
		return nil, err
	}

	// update the filter cursor
	fctx.cursor = p.chain.SnapshotCurrentCursor()
	return res, nil
}

func (p *proxyStub) getFilterContext(id DelegateFilterID) (*FilterContext, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	fctx, ok := p.delegates[id]
	return fctx, ok
}

// poll instantly polling filter changes from full node
func (p *proxyStub) poll() {
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	done := make(chan bool, 1)
	defer close(done)

	// To make the virtual filter service more resilient, we'd like it not take too long
	// to stop or reboot. Here start a monitor routine to kill the thread if timeout.
	p.shutdownCtx.Wg.Add(1)
	go func(fid web3rpc.ID) {
		defer p.shutdownCtx.Wg.Done()

		select {
		case <-p.shutdownCtx.Ctx.Done(): // on shutdown
			atomic.StoreInt32(&p.quitflag, 1)

			select {
			case <-time.After(rpcutil.DefaultShutdownTimeout):
				logrus.WithField("fid", fid).Info("Virtual proxy filter killed due to timeout")
				return
			case <-done:
				break
			}
		case <-done:
			break
		}

		logrus.WithField("fid", fid).Info("virtual proxy filter closed")

		if atomic.LoadInt32(&p.quitflag) != 0 {
			logrus.WithField("fnUrl", p.client.URL).Info("Virtual filter proxy stub shutdown")
		}
	}(p.fid)

	lastPollingTime := time.Now()

	for {
		select {
		case <-ticker.C:
			if p.gc() { // garbage collected?
				return
			}

			start := time.Now()
			ok, err := p.pollOnce(&lastPollingTime)
			metrics.Registry.VirtualFilter.PollOnceQps(p.client.NodeName(), err).UpdateSince(start)

			if !ok {
				p.close()
				return
			}

		case <-p.shutdownCtx.Ctx.Done():
			atomic.StoreInt32(&p.quitflag, 1)
			p.close()
			return
		}
	}
}

func (p *proxyStub) pollOnce(lastPollingTime *time.Time) (bool, error) {
	logger := logrus.WithFields(logrus.Fields{
		"proxyFullNode": p.client.URL,
		"proxyContext":  p.proxyContext,
	})

	fchanges, err := p.client.Filter.GetFilterChanges(p.fid)
	if err != nil {
		// proxy filter not found by full node? this may be due to full node reboot.
		if IsFilterNotFoundError(err) {
			logger.WithError(err).Info("Proxy filter already removed on full node side")
			return false, err
		}

		duration := time.Since(*lastPollingTime)
		if duration < maxPollingDelayDuration { // retry
			return true, err
		}

		logger.WithFields(logrus.Fields{
			"delayedDuration": duration,
		}).WithError(err).Error("Filter proxy failed to poll filter changes due to too long delays")
		return false, err
	}

	// merge the filter changes
	if err = p.merge(fchanges); err != nil {
		logger.WithError(err).Error("Filter proxy failed to merge filter changes")
		return false, err
	}

	// notify polled change data
	if p.obs != nil {
		if err := p.obs.onPolled(p, fchanges); err != nil {
			logger.WithError(err).Error("Filter proxy failed to notify polled change data")
			return false, errors.WithMessage(err, "poll event handled error")
		}
	}

	// update polling time
	*lastPollingTime = time.Now()

	// metric poll size
	pollsize := int64(len(fchanges.Logs))
	metrics.Registry.VirtualFilter.PollOnceSize(p.client.NodeName()).Update(pollsize)

	return true, nil
}

func (p *proxyStub) merge(changes *types.FilterChanges) error {
	chainedBlocksList := parseFilterChanges(changes)
	if len(chainedBlocksList) == 0 {
		return nil
	}

	metricTimer := metrics.Registry.VirtualFilter.PersistFilterChanges(p.client.NodeName(), "memory")
	defer metricTimer.Update()

	p.mu.Lock()
	defer p.mu.Unlock()

	// update the virtual filter blockchain
	for i := range chainedBlocksList {
		if head := chainedBlocksList[i].Front(); head.Value.(*FilterBlock).reorg {
			// reorg the chain
			if err := p.chain.Reorg(chainedBlocksList[i]); err != nil {
				return errors.WithMessage(err, "failed to reorg filter chain")
			}

			continue
		}

		// extend the chain
		if err := p.chain.Extend(chainedBlocksList[i]); err != nil {
			return errors.WithMessage(err, "failed to extend filter chain")
		}
	}

	return nil
}

// close closes the proxy so that new shared proxy filter can be recreated
func (p *proxyStub) close(lockfree ...bool) {
	if len(lockfree) == 0 || !lockfree[0] {
		p.mu.Lock()
		defer p.mu.Unlock()
	}

	if p.fid != nilRpcId {
		// uninstall the proxy filter with error ignored
		p.client.Filter.UninstallFilter(p.fid)
	}

	if p.obs != nil {
		p.obs.onClosed(p)
	}

	// reset proxy context
	p.proxyContext = nilProxyContext
}

// gc closes the proxy if not under use by any delegate filter
func (p *proxyStub) gc() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.delegates) == 0 {
		logrus.WithFields(logrus.Fields{
			"fnUrl":    p.client.URL,
			"filterID": p.fid,
		}).Info("Virtual filter garbage collects proxy stub due to no delegates")
		p.close(true)
		return true
	}

	return false
}

// establishes a new shared filter to full node if necessary
func (p *proxyStub) establish() error {
	if p.fid != nilRpcId { // already established
		return nil
	}

	// establishes a shared universal proxy filter to full node
	fid, err := p.client.Filter.NewLogFilter(&types.FilterQuery{})
	if err != nil {
		return err
	}

	p.proxyContext = newProxyContext(*fid, p.cfg.MaxFullFilterBlocks)

	if p.obs != nil {
		p.obs.onEstablished(p)
	}

	// start polling from full node instantly
	go p.poll()

	return nil
}
