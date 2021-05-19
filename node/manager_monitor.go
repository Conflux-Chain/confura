package node

import (
	"sort"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

// Implementations for HealthMonitor interface.

func (m *Manager) HealthyEpoch() uint64 {
	return atomic.LoadUint64(&m.midEpoch)
}

func (m *Manager) ReportEpoch(nodeName string, epoch uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodeName2Epochs[nodeName] = epoch

	var epochs UInt64Slice

	for _, epoch := range m.nodeName2Epochs {
		epochs = append(epochs, epoch)
	}

	sort.Sort(epochs)

	atomic.StoreUint64(&m.midEpoch, epochs[len(epochs)/2])
}

func (m *Manager) ReportUnhealthy(nodeName string, remind bool, reason error) {
	logger := logrus.WithError(reason).WithField("node", nodeName)

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

func (m *Manager) ReportHealthy(nodeName string) {
	// alert
	logrus.WithField("node", nodeName).Warn("Node became healthy now")

	// add recovered node into hash ring again
	m.hashRing.Add(m.nodes[nodeName])
}

type UInt64Slice []uint64

func (p UInt64Slice) Len() int           { return len(p) }
func (p UInt64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p UInt64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
