package node

import (
	"sort"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

// Implementations for HealthMonitor interface.

// HealthyEpoch returns the middle epoch height collected from managed cluster nodes,
// which is also regarded as the overall health epoch height.
func (m *Manager) HealthyEpoch() uint64 {
	return atomic.LoadUint64(&m.midEpoch)
}

// ReportEpoch reports latest epoch height of managed node to manager.
func (m *Manager) ReportEpoch(nodeName string, epoch uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodeName2Epochs[nodeName] = epoch
	if len(m.nodeName2Epochs) == 1 {
		atomic.StoreUint64(&m.midEpoch, epoch)
		return
	}

	var epochs []int
	for _, epoch := range m.nodeName2Epochs {
		epochs = append(epochs, int(epoch))
	}

	sort.Ints(epochs)

	atomic.StoreUint64(&m.midEpoch, uint64(epochs[len(epochs)/2]))
}

// ReportUnhealthy reports unhealthy status of managed node to manager.
func (m *Manager) ReportUnhealthy(nodeName string, remind bool, reason error) {
	logger := logrus.WithFields(logrus.Fields{
		"node":  nodeName,
		"group": m.group,
	}).WithError(reason)

	// alert
	if remind {
		logger.Error("Node not recovered")
	} else {
		logger.Error("Node became unhealthy")
	}

	// remove unhealthy node from hash ring
	m.hashRing.Remove(nodeName)

	// FIXME update repartition cache if configured
}

// ReportHealthy reports healthy status of managed node to manager.
func (m *Manager) ReportHealthy(nodeName string) {
	// alert
	logrus.WithField("node", nodeName).Warn("Node became healthy now")

	// add recovered node into hash ring again
	if n, ok := m.Get(nodeName); ok {
		m.hashRing.Add(n)
	} else { // this should not happen, but just in case
		logrus.WithField("node", nodeName).Error("Node not found in manager")
	}
}
