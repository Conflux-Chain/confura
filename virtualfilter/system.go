package virtualfilter

import (
	"context"
	"time"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

const (
	// filter change polling settings
	pollingInterval         = 1 * time.Second
	maxPollingDelayDuration = 1 * time.Minute
)

// filterSystemBase base struct for virtual filter system, which creates filter worker to
// establish proxy log filter to full node, and consistently polls event logs from the full node
// to persist data in db/cache store for high performance and stable log filter data retrieval service.
type filterSystemBase struct {
	filterMgr *filterManager     // virtual filter manager
	workers   util.ConcurrentMap // filter workers

	// log store to persist changed logs for more reliability
	logStore *mysql.VirtualFilterLogStore

	// graceful shutdown context
	shutdownCtx cmdutil.GracefulShutdownContext
}

func newFilterSystemBase(
	ttl time.Duration,
	vfls *mysql.VirtualFilterLogStore,
	shutdownCtx cmdutil.GracefulShutdownContext,
) *filterSystemBase {
	fs := &filterSystemBase{
		logStore:    vfls,
		shutdownCtx: shutdownCtx,
		filterMgr:   newFilterManager(),
	}

	shutdownCtx.Wg.Add(1)
	go fs.timeoutLoop(ttl)
	return fs
}

func (fs *filterSystemBase) getFilter(id rpc.ID) (virtualFilter, bool) {
	return fs.filterMgr.get(id)
}

// timeoutLoop runs at the interval set by 'ttl' and deletes expired virtual filters
func (fs *filterSystemBase) timeoutLoop(ttl time.Duration) {
	defer fs.shutdownCtx.Wg.Done()
	ticker := time.NewTicker(ttl / 2)
	defer ticker.Stop()

	for {
		select {
		case <-fs.shutdownCtx.Ctx.Done():
			return
		case <-ticker.C:
			expfs := fs.filterMgr.expire(ttl)
			for _, vf := range expfs {
				vf.uninstall()
			}
		}
	}
}

// implement `filterWorkerObserver` interface

func (fs *filterSystemBase) onEstablished(ctx context.Context, nodeName string, fid rpc.ID) error {
	// prepare partition table for changed logs persistence
	_, _, err := fs.logStore.WithContext(ctx).PreparePartition(string(fid))
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"nodeName": nodeName,
			"fid":      fid,
		}).WithError(err).Error("Filter system failed to prepare virtual filter log partition")
	}

	return err
}

func (fs *filterSystemBase) onClosed(ctx context.Context, nodeName string, fid rpc.ID) error {
	// clean all partition tables for the proxy filter
	err := fs.logStore.WithContext(ctx).DeletePartitions(string(fid))
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"nodeName": nodeName,
			"fid":      fid,
		}).WithError(err).Error("Filter system failed to clean virtual filter log partitions")
	}

	return err
}

func metricVirtualFilterSession(space string, f virtualFilter, delta int64) {
	switch f.ftype() {
	case filterTypeBlock:
		metrics.Registry.VirtualFilter.Sessions(space, "block", f.nodeName()).Inc(delta)
	case filterTypePendingTxn:
		metrics.Registry.VirtualFilter.Sessions(space, "pendingTxn", f.nodeName()).Inc(delta)
	case filterTypeLog:
		metrics.Registry.VirtualFilter.Sessions(space, "log", f.nodeName()).Inc(delta)
	default:
		return
	}
}
