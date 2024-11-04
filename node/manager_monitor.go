package node

import (
	"slices"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// monitorStatus is the monitor status of managed nodes.
type monitorStatus struct {
	epoch            uint64    // the latest epoch height
	unhealthy        bool      // whether the node is unhealthy
	unhealthReportAt time.Time // the last unhealthy report time
}

// Implementations for HealthMonitor interface.

// HealthyEpoch returns the middle epoch height collected from managed cluster nodes,
// which is also regarded as the overall health epoch height.
func (m *Manager) HealthyEpoch() uint64 {
	return atomic.LoadUint64(&m.midEpoch)
}

// HealthStatus checks the health status of a full node and returns whether it is unhealthy
// along with the time when it was reported as unhealthy.
//
// Parameters:
//   - nodeName: The name of the node to check.
//
// Returns:
//   - isUnhealthy: A boolean indicating if the node is unhealthy.
//   - reportedAt: The time when the node was reported as unhealthy.
func (m *Manager) HealthStatus(nodeName string) (isUnhealthy bool, reportedAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	status := m.monitorStatuses[nodeName]
	return status.unhealthy, status.unhealthReportAt
}

// ReportEpoch reports latest epoch height of managed node to manager.
func (m *Manager) ReportEpoch(nodeName string, epoch uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.updateEpoch(nodeName, epoch)

	if len(m.monitorStatuses) == 1 {
		atomic.StoreUint64(&m.midEpoch, epoch)
		return
	}

	var epochs []uint64
	for _, ms := range m.monitorStatuses {
		epochs = append(epochs, ms.epoch)
	}

	slices.Sort(epochs)

	atomic.StoreUint64(&m.midEpoch, epochs[len(epochs)/2])
}

func (m *Manager) updateEpoch(nodeName string, epoch uint64) {
	status := m.monitorStatuses[nodeName]
	status.epoch = epoch
	m.monitorStatuses[nodeName] = status
}

// ReportUnhealthy reports unhealthy status of managed node to manager.
func (m *Manager) ReportUnhealthy(nodeName string, remind bool, reason error) {
	m.updateUnhealthy(nodeName)

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

func (m *Manager) updateUnhealthy(nodeName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	status := m.monitorStatuses[nodeName]
	status.unhealthy = true
	status.unhealthReportAt = time.Now()
	m.monitorStatuses[nodeName] = status
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

func (m *Manager) updateHealthy(nodeName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	status := m.monitorStatuses[nodeName]
	status.unhealthy = false
	status.unhealthReportAt = time.Time{}
	m.monitorStatuses[nodeName] = status
}
