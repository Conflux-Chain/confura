package virtualfilter

import (
	"time"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	"github.com/Conflux-Chain/confura/store/mysql"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	gethmetrics "github.com/ethereum/go-ethereum/metrics"
	"github.com/openweb3/go-rpc-provider"
	"github.com/sirupsen/logrus"
)

const (
	// filter change polling settings
	pollingInterval         = 1 * time.Second
	maxPollingDelayDuration = 1 * time.Minute
)

// filterSystemBase base struct for virtual filter system, which creates filter worker to
// establish proxy log filter to full node, and consistantly polls event logs from the full node
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

	go fs.timeoutLoop(ttl)
	return fs
}

func (fs *filterSystemBase) getFilter(id rpc.ID) (virtualFilter, bool) {
	return fs.filterMgr.get(id)
}

// timeoutLoop runs at the interval set by 'ttl' and deletes expired virtual filters
func (fs *filterSystemBase) timeoutLoop(ttl time.Duration) {
	ticker := time.NewTicker(ttl / 2)
	defer ticker.Stop()

	for range ticker.C {
		expfs := fs.filterMgr.expire(ttl)
		for _, vf := range expfs {
			vf.uninstall()
		}
	}
}

// implement `filterWorkerObserver` interface

func (fs *filterSystemBase) onEstablished(nodeName string, fid rpc.ID) error {
	// prepare partition table for changed logs persistence
	_, _, err := fs.logStore.PreparePartition(string(fid))
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"nodeName": nodeName,
			"fid":      fid,
		}).WithError(err).Error("Filter system failed to prepare virtual filter log partition")
	}

	return err
}

func (fs *filterSystemBase) onClosed(nodeName string, fid rpc.ID) error {
	// clean all partition tables for the proxy filter
	err := fs.logStore.DeletePartitions(string(fid))
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"nodeName": nodeName,
			"fid":      fid,
		}).WithError(err).Error("Filter system failed to clean virtual filter log partitions")
	}

	return err
}

func metricVirtualFilterSession(space string, f virtualFilter, delta int64) {
	var gauge gethmetrics.Gauge

	switch f.ftype() {
	case filterTypeBlock:
		gauge = metrics.Registry.VirtualFilter.Sessions(space, "block", f.nodeName())
	case filterTypePendingTxn:
		gauge = metrics.Registry.VirtualFilter.Sessions(space, "pendingTxn", f.nodeName())
	case filterTypeLog:
		gauge = metrics.Registry.VirtualFilter.Sessions(space, "log", f.nodeName())
	default:
		return
	}

	gauge.Inc(delta)
}
