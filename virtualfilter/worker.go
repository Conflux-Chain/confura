package virtualfilter

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
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
)

// pollingSession session context data for the filer worker during polling
type pollingSession struct {
	// id of the shared proxy filter
	fid rpc.ID
	// last polling time
	lastPollingTime time.Time
	// delegate virtual filter cursors
	fcursors map[rpc.ID]filterCursor
	// simulated filter blockchain (for world outlook) to which
	// the polling will be applied
	fchain filterChain
}

// pollingClient client for filter worker to polling from full node
type pollingClient interface {
	// establish polling session
	establish() (pollingSession, error)
	// fetch changes from filter with specific id
	fetch(fid rpc.ID) (filterChanges, error)
	// uninstall filter with specific id
	uninstall(fid rpc.ID) (bool, error)
}

func newPollingSession(fid rpc.ID, chain filterChain) *pollingSession {
	return &pollingSession{
		fid:             fid,
		fchain:          chain,
		lastPollingTime: time.Now(),
		fcursors:        make(map[rpc.ID]filterCursor),
	}
}

// pollingObserver observes virtual filter worker polling
type pollingObserver interface {
	// on polling session established
	onEstablished(nodeName string, fid rpc.ID) error
	// on filter changes polled
	onPolled(nodeName string, fid rpc.ID, fchanges filterChanges) error
	// on polling session close
	onClosed(nodeName string, fid rpc.ID) error
}

// filterWorker consistently polls filter changes from full node and simulate
// a virtual filter chain for late retrieval.
type filterWorker struct {
	mu          sync.Mutex
	quitflag    uint32
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
	if atomic.LoadUint32(&w.quitflag) != 0 { // worker already shutdown
		return errFilterWorkerShutdown
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// establish filter polling session if not done yet
	if w.session.fid == nilRpcId {
		session, err := w.client.establish()
		if err != nil {
			return err
		}

		w.session = session

		if w.observer != nil {
			w.observer.onEstablished(w.nodeName, w.session.fid)
		}

		go w.poll()
	}

	// snapshot filter cursor for the delegate virtual filter
	w.session.fcursors[f.fid()] = w.session.fchain.snapshotLatestCursor()
	return nil
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
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	done := make(chan bool, 1)
	defer close(done)

	// graceful shutdown guard monitor
	go w.shutdownTimeoutGuard(w.session.fid, done)

	for {
		select {
		case <-ticker.C:
			if w.gc() { // garbage collected?
				return
			}

			start := time.Now()
			err := w.pollOnce()
			metrics.Registry.VirtualFilter.
				PollOnceQps(w.space, w.nodeName, err).UpdateSince(start)

			if err != nil {
				logrus.WithError(err).Info("Virtual filter session closed due to error")
				w.close()
				return
			}
		case <-w.shutdownCtx.Ctx.Done():
			atomic.StoreUint32(&w.quitflag, 1)
			w.close()
			return
		}
	}
}

func (w *filterWorker) pollOnce() error {
	logger := logrus.WithFields(logrus.Fields{
		"fid":      w.session.fid,
		"nodeName": w.nodeName,
	})

	fchanges, err := w.client.fetch(w.session.fid)
	if err != nil {
		// shared proxy filter not found by full node? this may be due to full node reboot
		if isFilterNotFoundError(err) {
			return err
		}

		duration := time.Since(w.session.lastPollingTime)
		if duration < maxPollingDelayDuration { // retry for fault tolerance
			logger.WithError(err).Info("Filter worker client failed to poll filter changes")
			return nil
		}

		return err
	}

	// merge the changes to the filter chain
	if err := w.merge(fchanges); err != nil {
		// logging the invalid polled filter changes for diagonistics
		fcJsonStr, _ := json.Marshal(fchanges)
		logger.WithField("filterChangesJson", string(fcJsonStr)).Info("Invalid filter changes to merge")

		w.dumpFilterChain()

		logger.WithError(err).Error("Filter worker failed to merge filter chain")
		return err
	}

	// notify polled changes
	if w.observer != nil {
		if err := w.observer.onPolled(w.nodeName, w.session.fid, fchanges); err != nil {
			return errors.WithMessage(err, "poll event handling error by observer")
		}
	}

	// update last polling time
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

// To make the virtual filter service more resilient, we'd like it not take too long
// to stop or reboot. Here starts a monitor routine to kill the thread if timeout.
func (w *filterWorker) shutdownTimeoutGuard(fid rpc.ID, done <-chan bool) {
	w.shutdownCtx.Wg.Add(1)
	defer w.shutdownCtx.Wg.Done()

	select {
	case <-w.shutdownCtx.Ctx.Done(): // on shutdown
		atomic.StoreUint32(&w.quitflag, 1)

		select {
		case <-time.After(rpcutil.DefaultShutdownTimeout):
			logrus.WithFields(logrus.Fields{
				"nodeName": w.nodeName,
				"fid":      fid,
			}).Info("Virtual filter worker killed due to timeout")
			return
		case <-done:
			break
		}
	case <-done:
		break
	}
}

// garbage collects by closing the filter session if idle
func (w *filterWorker) gc() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.session.fcursors) == 0 {
		// idle if no virtual filter delegated
		logrus.WithField("fid", w.session.fid).
			Info("Virtual filter session closed due to idle")

		w.close(true)
		return true
	}

	return false
}

// close the polling session of filter worker
func (w *filterWorker) close(lockfree ...bool) {
	if len(lockfree) == 0 || !lockfree[0] {
		w.mu.Lock()
		defer w.mu.Unlock()
	}

	fid := w.session.fid
	w.client.uninstall(fid)

	// reset polling session
	w.session = nilPollingSession

	if w.observer != nil {
		w.observer.onClosed(w.nodeName, fid)
	}
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
func (w *ethFilterWorker) fetchPollingChanges(fid rpc.ID) (*ethPollingChanges, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	cursor, ok := w.session.fcursors[fid]
	if !ok {
		return nil, errFilterNotFound
	}

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
		if node.cursor() == cursor && !block.reverted {
			return true
		}

		fblocks = append(fblocks, *block)
		return true
	})

	if err != nil {
		return nil, err
	}

	// update the filter cursor
	w.session.fcursors[fid] = w.session.fchain.snapshotLatestCursor()

	pchanges := &ethPollingChanges{
		fid: w.session.fid, blocks: fblocks,
	}
	return pchanges, nil
}

// implements `pollingClient` interface

func (w *ethFilterWorker) establish() (pollingSession, error) {
	fid, err := w.client.Filter.NewLogFilter(&ethtypes.FilterQuery{})
	if err != nil {
		return nilPollingSession, err
	}

	session := newPollingSession(*fid, newEthFilterChain(w.maxFullFilterBlocks))
	return *session, nil
}

func (w *ethFilterWorker) fetch(fid rpc.ID) (filterChanges, error) {
	// set as much timeout as possible for more fault tolerance
	timeoutCtx, cancel := context.WithTimeout(context.Background(), maxPollingDelayDuration)
	defer cancel()

	var fchanges *ethtypes.FilterChanges
	err := w.client.Filter.CallContext(timeoutCtx, &fchanges, "eth_getFilterChanges", fid)
	return fchanges, err
}

func (w *ethFilterWorker) uninstall(fid rpc.ID) (bool, error) {
	return w.client.Filter.UninstallFilter(fid)
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

func (w *cfxFilterWorker) establish() (pollingSession, error) {
	fid, err := w.client.Filter().NewFilter(cfxtypes.LogFilter{})
	if err != nil {
		return nilPollingSession, err
	}

	session := newPollingSession(*fid, newCfxFilterChain(w.maxFullFilterEpochs))
	return *session, nil
}

func (w *cfxFilterWorker) fetch(fid rpc.ID) (filterChanges, error) {
	// set as much timeout as possible for more fault tolerance
	timeoutCtx, cancel := context.WithTimeout(context.Background(), maxPollingDelayDuration)
	defer cancel()

	var fchanges *cfxtypes.CfxFilterChanges
	err := w.client.CallContext(timeoutCtx, &fchanges, "cfx_getFilterChanges", fid)
	return fchanges, err
}

func (w *cfxFilterWorker) uninstall(fid rpc.ID) (bool, error) {
	return w.client.Filter().UninstallFilter(fid)
}

type cfxPollingChanges struct {
	fid    rpc.ID           // proxy filter where changes are polled
	epochs []cfxFilterEpoch // changed epochs since last polling
}

// fetchPollingChanges fetch filter changes since last polling
func (w *cfxFilterWorker) fetchPollingChanges(fid rpc.ID) (*cfxPollingChanges, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	cursor, ok := w.session.fcursors[fid]
	if !ok {
		return nil, errFilterNotFound
	}

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
		if node.cursor() == cursor && !epoch.reorged() {
			return true
		}

		fepochs = append(fepochs, *epoch)
		return true
	})

	if err != nil {
		return nil, err
	}

	// update the filter cursor
	w.session.fcursors[fid] = w.session.fchain.snapshotLatestCursor()

	pchanges := &cfxPollingChanges{
		fid: w.session.fid, epochs: fepochs,
	}
	return pchanges, nil
}
