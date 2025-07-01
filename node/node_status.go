package node

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Conflux-Chain/confura/util/metrics"
	metricUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	ring "github.com/zealws/golang-ring"
)

// HealthMonitor is implemented by any objects that support to monitor full node health.
type HealthMonitor interface {
	// HealthyEpoch returns the healthy epoch number among full nodes.
	// Usually, it is the middle epoch number of all full nodes.
	HealthyEpoch() uint64

	// HealthStatus checks the health status of a full node and returns whether it is unhealthy
	// along with the time when it was reported as unhealthy.
	HealthStatus(nodeName string) (isUnhealthy bool, reportedAt time.Time)

	// ReportEpoch fired when epoch changes.
	ReportEpoch(nodeName string, epoch uint64)

	// ReportUnhealthy fired when full node becomes unhealthy or unrecovered for a long time.
	ReportUnhealthy(nodeName string, remind bool, reason error)

	// ReportHealthy fired when full node becomes healthy.
	ReportHealthy(nodeName string)
}

// Status represents the node status, including current epoch number and health status.
type Status struct {
	nodeName string

	metric *statusMetrics

	latestStateEpoch uint64
	successCounter   uint64
	failureCounter   uint64

	latestHeartBeatErrs *ring.Ring
}

func NewStatus(group Group, nodeName string) Status {
	hbErrRingBuf := &ring.Ring{}
	hbErrRingBuf.SetCapacity(int(2 * cfg.Monitor.Unhealth.Failures))

	return Status{
		nodeName: nodeName,
		metric: newStatusMetrics(
			metrics.Registry.Nodes.NodeLatency(group.Space(), group.String(), nodeName),
			metrics.Registry.Nodes.NodeAvailability(group.Space(), group.String(), nodeName),
		),
		latestHeartBeatErrs: hbErrRingBuf,
	}
}

// Update heartbeats with node and updates health status.
func (s *Status) Update(n Node) {
	s.heartbeat(n)
}

// Report reports health status to monitor.
func (s *Status) Report(hm HealthMonitor) {
	s.updateHealth(hm)
}

// MarshalJSON marshals as JSON.
func (s *Status) MarshalJSON() ([]byte, error) {
	type Status struct {
		NodeName string `json:"nodeName"`

		Availability string `json:"availability"`

		MeanLatency string `json:"meanLatency"`
		P99Latency  string `json:"P99Latency"`
		P75Latency  string `json:"P75Latency"`

		LatestStateEpoch uint64 `json:"latestStateEpoch"`
		SuccessCounter   uint64 `json:"successCounter"`
		FailureCounter   uint64 `json:"failureCounter"`

		LatestHeartBeatErrs []string `json:"latestHeartBeatErrs"`
	}

	availability := metricUtil.GetOrRegisterTimeWindowPercentageDefault(s.metric.availability).Value()
	latency := metricUtil.GetOrRegisterHistogram(s.metric.latency).Snapshot()

	scopy := Status{
		NodeName:         s.nodeName,
		Availability:     fmt.Sprintf("%.2f%%", availability),
		MeanLatency:      fmt.Sprintf("%.2f(ms)", latency.Mean()/1e6),
		P99Latency:       fmt.Sprintf("%.2f(ms)", latency.Percentile(0.99)/1e6),
		P75Latency:       fmt.Sprintf("%.2f(ms)", latency.Percentile(0.75)/1e6),
		LatestStateEpoch: s.latestStateEpoch,
		SuccessCounter:   s.successCounter,
		FailureCounter:   s.failureCounter,
	}

	hbErrors := s.latestHeartBeatErrs.Values()
	for _, e := range hbErrors {
		scopy.LatestHeartBeatErrs = append(scopy.LatestHeartBeatErrs, e.(error).Error())
	}

	return json.Marshal(&scopy)
}

// heartbeat heartbeats with node to update status.
func (s *Status) heartbeat(n Node) {
	start := time.Now()
	epoch, err := n.LatestEpochNumber()
	s.metric.update(start, err)
	if err != nil {
		s.failureCounter++
		s.successCounter = 0

		logrus.WithFields(logrus.Fields{
			"status": s, "reqTime": start,
		}).WithError(err).Info("Failed to heartbeat with node")
		s.latestHeartBeatErrs.Enqueue(err)
	} else {
		s.latestStateEpoch = epoch
		s.failureCounter = 0
		s.successCounter++
	}
}

// updateHealth reports health status to monitor.
func (s *Status) updateHealth(monitor HealthMonitor) {
	reason := s.checkHealth(monitor.HealthyEpoch())
	unhealthy, unhealthReportAt := monitor.HealthStatus(s.nodeName)

	if unhealthy {
		if reason == nil {
			// node become healthy after N success
			if s.successCounter >= cfg.Monitor.Recover.SuccessCounter {
				monitor.ReportHealthy(s.nodeName)
			}
		} else {
			// remind long unhealthy every N minutes, even occasionally succeeded
			remindTime := unhealthReportAt.Add(cfg.Monitor.Recover.RemindInterval)
			if now := time.Now(); now.After(remindTime) {
				monitor.ReportUnhealthy(s.nodeName, true, reason)
			}
		}
	} else {
		if reason == nil {
			monitor.ReportEpoch(s.nodeName, s.latestStateEpoch)
		} else {
			// node become unhealthy
			monitor.ReportUnhealthy(s.nodeName, false, reason)
		}
	}
}

// checkHealth checks health status with collected node information.
func (s *Status) checkHealth(targetEpoch uint64) error {
	// RPC failures
	if s.failureCounter >= cfg.Monitor.Unhealth.Failures {
		return errors.Errorf("RPC failures (%v)", s.failureCounter)
	}

	// epoch fall behind
	if s.latestStateEpoch+cfg.Monitor.Unhealth.EpochsFallBehind < targetEpoch {
		return errors.Errorf("Epoch fall behind (%v)", targetEpoch-s.latestStateEpoch)
	}

	// latency too high
	percentile := cfg.Monitor.Unhealth.LatencyPercentile
	maxLatency := cfg.Monitor.Unhealth.MaxLatency
	latencySnapshot := metricUtil.GetOrRegisterHistogram(s.metric.latency).Snapshot()
	latency := time.Duration(latencySnapshot.Percentile(percentile))
	if latency > maxLatency {
		return errors.Errorf("Latency too high (%v)", latency)
	}

	return nil
}

func (s *Status) Close() {
	s.metric.unregisterAll()
}

type statusMetrics struct {
	latency      string // ping latency via cfx_epochNumber/eth_blockNumber
	availability string // node availability percent
}

func newStatusMetrics(latency, availability string) *statusMetrics {
	return &statusMetrics{
		latency:      latency,
		availability: availability,
	}
}

func (sm *statusMetrics) update(start time.Time, err error) {
	if err == nil {
		metricUtil.GetOrRegisterHistogram(sm.latency).Update(time.Since(start).Nanoseconds())
	}

	metricUtil.GetOrRegisterTimeWindowPercentageDefault(sm.availability).Mark(err == nil)
}

func (sm *statusMetrics) unregisterAll() {
	metrics.Unregister(sm.latency)
	metrics.Unregister(sm.availability)
}
