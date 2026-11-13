package virtualfilter

import (
	"context"
	"sync"
	"time"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/util/metrics"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider"
	ethtypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	nilPollingSession = pollingSession{fid: nilRpcId}

	errFilterWorkerShutdown = errors.New("filter worker already shutdown")
	errFilterCleanupPending = errors.New("filter session cleanup pending")
)

// pollingSession session context data for the filer worker during polling
type pollingSession struct {
	// id of the shared proxy filter
	fid rpc.ID
	// last polling time
	lastPollingTime time.Time
	// delegate virtual filter cursors
	fcursors map[rpc.ID]*filterDelegate
	// simulated filter blockchain (for world outlook) to which
	// the polling will be applied
	fchain filterChain
}

// pollingClient client for filter worker to polling from full node
type pollingClient interface {
	// establish polling session
	establish(ctx context.Context) (pollingSession, error)
	// fetch changes from filter with specific id
	fetch(ctx context.Context, fid rpc.ID) (filterChanges, error)
	// uninstall filter with specific id
	uninstall(ctx context.Context, fid rpc.ID) (bool, error)
}

func newPollingSession(fid rpc.ID, chain filterChain) *pollingSession {
	return &pollingSession{
		fid:             fid,
		fchain:          chain,
		lastPollingTime: time.Now(),
		fcursors:        make(map[rpc.ID]*filterDelegate),
	}
}

// pollingObserver observes virtual filter worker polling
type pollingObserver interface {
	// on polling session established
	onEstablished(ctx context.Context, nodeName string, fid rpc.ID) error
	// on filter changes polled
	onPolled(ctx context.Context, nodeName string, fid rpc.ID, fchanges filterChanges) error
	// on polling session close
	onClosed(ctx context.Context, nodeName string, fid rpc.ID) error
}

// filterWorker consistently polls filter changes from full node and simulate
// a virtual filter chain for late retrieval.
type filterWorker struct {
	mu          sync.RWMutex
	running     bool
	closing     *closingSession
	shutdownCtx cmdutil.GracefulShutdownContext

	space    string          // network space
	nodeName string          // full node name
	session  pollingSession  // ongoing polling session
	client   pollingClient   // polling client
	observer pollingObserver // polling observer
}

func newFilterWorker(
	space, nodeName string,
	client pollingClient,
	obs pollingObserver,
	shutdownCtx cmdutil.GracefulShutdownContext,
) *filterWorker {
	return &filterWorker{
		observer:    obs,
		space:       space,
		nodeName:    nodeName,
		client:      client,
		session:     nilPollingSession,
		shutdownCtx: shutdownCtx,
	}
}

// accept accepts delegate for virtual filter
func (w *filterWorker) accept(f virtualFilter) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.shutdownCtx.Ctx.Err() != nil {
		return errFilterWorkerShutdown
	}
	if w.closing != nil {
		return errFilterCleanupPending
	}
	if w.session.fid == nilRpcId {
		ctx, cancel := context.WithTimeout(w.shutdownCtx.Ctx, maxPollingDelayDuration)
		defer cancel()
		session, err := w.client.establish(ctx)
		if err != nil {
			return err
		}
		w.session = session
		if w.observer != nil {
			if err := w.observer.onEstablished(ctx, w.nodeName, session.fid); err != nil {
				w.closeLocked()
				w.startPollingLocked()
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			w.closeLocked()
			w.startPollingLocked()
			return err
		}
	}
	if w.session.fchain == nil {
		return errFilterCleanupPending
	}
	w.session.fcursors[f.fid()] = &filterDelegate{cursor: w.session.fchain.snapshotLatestCursor()}
	w.startPollingLocked()
	return nil
}

func (w *filterWorker) startPollingLocked() {
	if !w.running {
		w.running = true
		w.shutdownCtx.Wg.Add(1)
		go w.poll()
	}
}

// reject rejects delegate for virtual filter
func (w *filterWorker) reject(f virtualFilter) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.session.fcursors[f.fid()]; ok {
		delete(w.session.fcursors, f.fid())
		return true, nil
	}

	return false, nil
}

// poll consistently polls filter changes from full node and applies the polled data
// to the current polling session.
func (w *filterWorker) poll() {
	defer w.shutdownCtx.Wg.Done()
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.shutdownCtx.Ctx.Done():
			w.close()
			ctx, cancel := context.WithTimeout(context.Background(), rpcutil.DefaultShutdownTimeout)
			w.retryCleanup(ctx, true)
			cancel()
			w.mu.Lock()
			if w.closing != nil {
				logrus.WithField("fid", w.closing.fid).Warn("Virtual filter cleanup incomplete at shutdown")
			}
			w.running = false
			w.mu.Unlock()
			return
		case <-ticker.C:
			w.mu.Lock()
			if w.session.fid != nilRpcId && len(w.session.fcursors) == 0 {
				w.closeLocked()
			}
			closing := w.closing != nil
			if !closing && w.session.fid == nilRpcId {
				w.running = false
				w.mu.Unlock()
				return
			}
			w.mu.Unlock()
			if closing {
				w.retryCleanup(w.shutdownCtx.Ctx, false)
				continue
			}
			start := time.Now()
			err := w.pollOnce()
			metrics.Registry.VirtualFilter.PollOnceQps(w.space, w.nodeName, err).UpdateSince(start)
			if err != nil {
				logrus.WithError(err).Info("Virtual filter session closed due to error")
				w.close()
				w.retryCleanup(w.shutdownCtx.Ctx, false)
			}
		}
	}
}

func (w *filterWorker) pollOnce() (result error) {
	w.mu.RLock()
	fid, lastPollingTime := w.session.fid, w.session.lastPollingTime
	w.mu.RUnlock()
	fchanges, err := w.client.fetch(w.shutdownCtx.Ctx, fid)
	if err != nil && !isFilterNotFoundError(err) && !errors.Is(err, errInvalidFilterChanges) && time.Since(lastPollingTime) < maxPollingDelayDuration {
		logrus.WithError(err).Info("Filter worker client failed to poll filter changes")
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.session.fid != fid || w.session.fchain == nil {
		return errFilterNotFound
	}
	// An upstream read is destructive. Any failure after it succeeds invalidates
	// this session before readers can see a partial persistent or chain view.
	defer func() {
		if result != nil {
			w.closeLocked()
		}
	}()
	if err != nil {
		return err
	}
	if err := validateLogChanges(fchanges); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(w.shutdownCtx.Ctx, maxPollingDelayDuration)
	defer cancel()
	if w.observer != nil {
		if err := w.observer.onPolled(ctx, w.nodeName, fid, fchanges); err != nil {
			return errors.WithMessage(err, "poll event handling error by observer")
		}
	}
	start := time.Now()
	err = w.session.fchain.merge(fchanges)
	metrics.Registry.VirtualFilter.PersistFilterChanges(w.space, w.nodeName, "memory").UpdateSince(start)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w.session.lastPollingTime = time.Now()
	return nil
}

func (w *filterWorker) dumpFilterChain() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if c := w.session.fchain; c != nil {
		c.print(nilFilterCursor)
	}
}

func (w *filterWorker) merge(fchanges filterChanges) error {
	startTime := time.Now()
	defer metrics.Registry.VirtualFilter.PersistFilterChanges(w.space, w.nodeName, "memory").UpdateSince(startTime)

	w.mu.Lock()
	defer w.mu.Unlock()

	if c := w.session.fchain; c != nil {
		return c.merge(fchanges)
	}

	return nil
}

// closingSession retains ownership until both independent cleanup steps succeed.
// It deliberately holds no chain or delegate data and there is at most one per worker.
type closingSession struct {
	fid                     rpc.ID
	upstreamDone, storeDone bool
	busy                    bool
	retryAt                 time.Time
	delay                   time.Duration
}

func (w *filterWorker) closeLocked() {
	if w.session.fid == nilRpcId {
		return
	}
	w.closing = &closingSession{fid: w.session.fid, storeDone: w.observer == nil}
	w.session = nilPollingSession
}

func (w *filterWorker) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeLocked()
}

// I/O runs without the data lock; busy and identity checks serialize attempts.
func (w *filterWorker) retryCleanup(parent context.Context, force bool) {
	w.mu.Lock()
	c := w.closing
	if c == nil || c.busy || (!force && time.Now().Before(c.retryAt)) {
		w.mu.Unlock()
		return
	}
	c.busy = true
	upstreamDone, storeDone := c.upstreamDone, c.storeDone
	w.mu.Unlock()
	ctx, cancel := context.WithTimeout(parent, rpcutil.DefaultShutdownTimeout)
	defer cancel()
	if !upstreamDone {
		_, err := w.client.uninstall(ctx, c.fid)
		upstreamDone = err == nil || isFilterNotFoundError(err)
		if !upstreamDone {
			logrus.WithError(err).WithField("fid", c.fid).Warn("Virtual filter upstream cleanup failed")
		}
	}
	if !storeDone {
		err := w.observer.onClosed(ctx, w.nodeName, c.fid)
		storeDone = err == nil
		if !storeDone {
			logrus.WithError(err).WithField("fid", c.fid).Warn("Virtual filter partition cleanup failed")
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing != c {
		return
	}
	c.upstreamDone, c.storeDone, c.busy = upstreamDone, storeDone, false
	if upstreamDone && storeDone {
		w.closing = nil
		return
	}
	if c.delay == 0 {
		c.delay = pollingInterval
	} else {
		c.delay = min(2*c.delay, 30*time.Second)
	}
	c.retryAt = time.Now().Add(c.delay)
}

// evm space filter worker
type ethFilterWorker struct {
	*filterWorker

	maxFullFilterBlocks int
	client              *node.Web3goClient
}

func newEthFilterWorker(
	maxFullFilterBlocks int,
	obs pollingObserver,
	client *node.Web3goClient,
	shutdownCtx cmdutil.GracefulShutdownContext,
) *ethFilterWorker {
	w := &ethFilterWorker{
		client:              client,
		maxFullFilterBlocks: maxFullFilterBlocks,
	}

	w.filterWorker = newFilterWorker(
		"eth", client.NodeName(), w, obs, shutdownCtx,
	)

	return w
}

type ethPollingChanges struct {
	fid    rpc.ID           // proxy filter where changes are polled
	blocks []ethFilterBlock // changed blocks since last polling
}

// fetchPollingChanges fetch filter changes since last polling
func (w *ethFilterWorker) fetchPollingChanges(cursor filterCursor) (*ethPollingChanges, error) {
	startTime := time.Now()
	defer metrics.Registry.VirtualFilter.QueryFilterChanges("eth", w.nodeName, "memory").UpdateSince(startTime)

	var fblocks []ethFilterBlock
	err := w.session.fchain.traverse(cursor, func(node *filterNode, forkPoint bool) bool {
		if node == nil { // skip nil node
			return true
		}

		block := node.filterNodable.(*ethFilterBlock)

		if cursor == nilFilterCursor { // full traversal for nil cursor
			fblocks = append(fblocks, *block)
			return true
		}

		// otherwise for non-nil cursor

		if forkPoint { // ignore fork point
			return true
		}

		// skip node at the filter cursor, whose block is not re-organized
		if node.cursor().sameNode(cursor) && !block.reverted {
			return true
		}

		fblocks = append(fblocks, *block)
		return true
	})

	if err != nil {
		return nil, err
	}

	pchanges := &ethPollingChanges{
		fid: w.session.fid, blocks: fblocks,
	}
	return pchanges, nil
}

// implements `pollingClient` interface

func (w *ethFilterWorker) establish(ctx context.Context) (pollingSession, error) {
	var fid *rpc.ID
	err := w.client.Filter.CallContext(ctx, &fid, "eth_newFilter", ethtypes.FilterQuery{})
	if err != nil {
		return nilPollingSession, err
	}

	if fid == nil || *fid == nilRpcId {
		return nilPollingSession, errInvalidFilterChanges
	}

	session := newPollingSession(*fid, newEthFilterChain(w.maxFullFilterBlocks))
	return *session, nil
}

func (w *ethFilterWorker) fetch(ctx context.Context, fid rpc.ID) (filterChanges, error) {
	// set as much timeout as possible for more fault tolerance
	timeoutCtx, cancel := context.WithTimeout(ctx, maxPollingDelayDuration)
	defer cancel()

	var raw upstreamLogResult
	err := w.client.Filter.CallContext(timeoutCtx, &raw, "eth_getFilterChanges", fid)
	if err != nil {
		return nil, err
	}
	var fchanges *ethtypes.FilterChanges
	err = decodeUpstreamLogResult(raw, &fchanges)
	return fchanges, err
}

func (w *ethFilterWorker) uninstall(ctx context.Context, fid rpc.ID) (bool, error) {
	var removed bool
	err := w.client.Filter.CallContext(ctx, &removed, "eth_uninstallFilter", fid)
	return removed, err
}

// core space filter worker
type cfxFilterWorker struct {
	*filterWorker

	maxFullFilterEpochs int
	client              *sdk.Client
}

func newCfxFilterWorker(
	maxFullFilterEpochs int,
	obs pollingObserver,
	client *sdk.Client,
	shutdownCtx cmdutil.GracefulShutdownContext,
) *cfxFilterWorker {
	w := &cfxFilterWorker{
		client:              client,
		maxFullFilterEpochs: maxFullFilterEpochs,
	}

	nodeName := rpcutil.Url2NodeName(client.GetNodeURL())
	w.filterWorker = newFilterWorker(
		"cfx", nodeName, w, obs, shutdownCtx,
	)

	return w
}

// implements `pollingClient` interface

func (w *cfxFilterWorker) establish(ctx context.Context) (pollingSession, error) {
	var fid *rpc.ID
	err := w.client.CallContext(ctx, &fid, "cfx_newFilter", cfxtypes.LogFilter{})
	if err != nil {
		return nilPollingSession, err
	}

	if fid == nil || *fid == nilRpcId {
		return nilPollingSession, errInvalidFilterChanges
	}

	session := newPollingSession(*fid, newCfxFilterChain(w.maxFullFilterEpochs))
	return *session, nil
}

func (w *cfxFilterWorker) fetch(ctx context.Context, fid rpc.ID) (filterChanges, error) {
	// set as much timeout as possible for more fault tolerance
	timeoutCtx, cancel := context.WithTimeout(ctx, maxPollingDelayDuration)
	defer cancel()

	var raw upstreamLogResult
	err := w.client.CallContext(timeoutCtx, &raw, "cfx_getFilterChanges", fid)
	if err != nil {
		return nil, err
	}
	var fchanges *cfxtypes.CfxFilterChanges
	err = decodeUpstreamLogResult(raw, &fchanges)
	return fchanges, err
}

func (w *cfxFilterWorker) uninstall(ctx context.Context, fid rpc.ID) (bool, error) {
	var removed bool
	err := w.client.CallContext(ctx, &removed, "cfx_uninstallFilter", fid)
	return removed, err
}

type cfxPollingChanges struct {
	fid    rpc.ID           // proxy filter where changes are polled
	epochs []cfxFilterEpoch // changed epochs since last polling
}

// fetchPollingChanges fetch filter changes since last polling
func (w *cfxFilterWorker) fetchPollingChanges(cursor filterCursor) (*cfxPollingChanges, error) {
	startTime := time.Now()
	defer metrics.Registry.VirtualFilter.PersistFilterChanges(w.space, w.nodeName, "cfx").UpdateSince(startTime)

	var fepochs []cfxFilterEpoch
	err := w.session.fchain.traverse(cursor, func(node *filterNode, forkPoint bool) bool {
		if node == nil { // skip nil node
			return true
		}

		epoch := node.filterNodable.(*cfxFilterEpoch)

		if cursor == nilFilterCursor { // full traversal for nil cursor
			fepochs = append(fepochs, *epoch)
			return true
		}

		// otherwise for non-nil cursor

		if forkPoint { // ignore fork point
			return true
		}

		// skip node at the filter cursor, whose epoch is not re-organized
		if node.cursor().sameNode(cursor) && !epoch.reorged() {
			return true
		}

		fepochs = append(fepochs, *epoch)
		return true
	})

	if err != nil {
		return nil, err
	}

	// Keep the most recent compensation even when deeper reverts arrive after
	// the canonical chain became empty. Collapse it with the fork prefix.
	chain := w.session.fchain.(*cfxFilterChain)
	if chain.latestRevert != nil && chain.revertRevision > cursor.revision {
		i := 0
		for i < len(fepochs) && fepochs[i].reorged() {
			i++
		}
		fepochs = append([]cfxFilterEpoch{*chain.latestRevert}, fepochs[i:]...)
	}

	pchanges := &cfxPollingChanges{
		fid: w.session.fid, epochs: fepochs,
	}
	return pchanges, nil
}

// Cursor fields are protected by the delegate mutex. Map membership and the
// chain view are protected by the worker mutex. Writers never lock delegates.
type filterDelegate struct {
	mu     sync.Mutex
	cursor filterCursor
}

func (w *filterWorker) beginRead(ctx context.Context, fid rpc.ID) (*filterDelegate, func(), error) {
	w.mu.RLock()
	delegate := w.session.fcursors[fid]
	w.mu.RUnlock()
	if delegate == nil {
		return nil, nil, errFilterNotFound
	}
	delegate.mu.Lock()
	w.mu.RLock()
	unlock := func() { w.mu.RUnlock(); delegate.mu.Unlock() }
	if w.session.fcursors[fid] != delegate {
		unlock()
		return nil, nil, errFilterNotFound
	}
	if err := ctx.Err(); err != nil {
		unlock()
		return nil, nil, err
	}
	return delegate, unlock, nil
}
