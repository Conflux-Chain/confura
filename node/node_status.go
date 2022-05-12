package node

import (
	"encoding/json"
	"fmt"
	"time"

	infuraMetrics "github.com/conflux-chain/conflux-infura/metrics"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	ring "github.com/zealws/golang-ring"
)

// HealthMonitor is implemented by any objects that support to monitor full node health.
type HealthMonitor interface {
	// HealthyEpoch returns the healthy epoch number among full nodes.
	// Usually, it is the middle epoch number of all full nodes.
	HealthyEpoch() uint64

	// ReportEpoch fired when epoch changes.
	ReportEpoch(nodeName string, epoch uint64)

	// ReportUnhealthy fired when full node becomes unhealthy or unrecovered for a long time.
	ReportUnhealthy(nodeName string, remind bool, reason error)

	// ReportHealthy fired when full node becomes healthy.
	ReportHealthy(nodeName string)
}

// Status represents the node status, including current epoch number and health.
type Status struct {
	nodeName string

	metricName    string
	latencyMetric metrics.Histogram // ping latency via cfx_epochNumber/eth_blockNumber

	latestStateEpoch uint64
	successCounter   uint64
	failureCounter   uint64
	unhealthy        bool
	unhealthReportAt time.Time

	latestHeartBeatErrs *ring.Ring
}

func NewStatus(nodeName string) Status {
	metricName := fmt.Sprintf("infura/nodes/latency/%v", nodeName)
	return newStatus(metricName, nodeName)
}

func NewEthStatus(nodeName string) Status {
	metricName := fmt.Sprintf("infura/ethnodes/latency/%v", nodeName)
	return newStatus(metricName, nodeName)
}

func newStatus(metricName, nodeName string) Status {
	hbErrRingBuf := &ring.Ring{}
	hbErrRingBuf.SetCapacity(int(2 * cfg.Monitor.Unhealth.Failures))

	return Status{
		nodeName:            nodeName,
		metricName:          metricName,
		latencyMetric:       infuraMetrics.GetOrRegisterHistogram(nil, metricName),
		latestHeartBeatErrs: hbErrRingBuf,
	}
}

func (s *Status) Update(n Node, monitor HealthMonitor) {
	s.heartbeat(n)
	s.updateHealth(monitor)
}

// MarshalJSON marshals as JSON.
func (s *Status) MarshalJSON() ([]byte, error) {
	type Status struct {
		NodeName string `json:"nodeName"`

		MeanLatency string `json:"meanLatency"`
		P99Latency  string `json:"P99Latency"`
		P75Latency  string `json:"P75Latency"`

		LatestStateEpoch uint64 `json:"latestStateEpoch"`
		SuccessCounter   uint64 `json:"successCounter"`
		FailureCounter   uint64 `json:"failureCounter"`
		Unhealthy        bool   `json:"unhealthy"`
		UnhealthReportAt string `json:"unhealthReportAt"`

		LatestHeartBeatErrs []string `json:"latestHeartBeatErrs"`
	}

	scopy := Status{
		NodeName:         s.nodeName,
		MeanLatency:      fmt.Sprintf("%.2f(ms)", s.latencyMetric.Mean()/1e6),
		P99Latency:       fmt.Sprintf("%.2f(ms)", s.latencyMetric.Percentile(0.99)/1e6),
		P75Latency:       fmt.Sprintf("%.2f(ms)", s.latencyMetric.Percentile(0.75)/1e6),
		LatestStateEpoch: s.latestStateEpoch,
		SuccessCounter:   s.successCounter,
		FailureCounter:   s.failureCounter,
		Unhealthy:        s.unhealthy,
		UnhealthReportAt: s.unhealthReportAt.Format(time.RFC3339),
	}

	hbErrors := s.latestHeartBeatErrs.Values()
	for _, e := range hbErrors {
		scopy.LatestHeartBeatErrs = append(scopy.LatestHeartBeatErrs, e.(error).Error())
	}

	return json.Marshal(&scopy)
}

func (s *Status) heartbeat(n Node) {
	start := time.Now()
	epoch, err := n.LatestEpochNumber()
	if err != nil {
		s.failureCounter++
		s.successCounter = 0

		logrus.WithFields(logrus.Fields{
			"status": s, "reqTime": start,
		}).WithError(err).Info("Failed to heartbeat with node")
		s.latestHeartBeatErrs.Enqueue(err)
	} else {
		s.latencyMetric.Update(time.Since(start).Nanoseconds())
		s.latestStateEpoch = epoch
		s.failureCounter = 0
		s.successCounter++
	}
}

func (s *Status) updateHealth(monitor HealthMonitor) {
	reason := s.checkHealth(monitor.HealthyEpoch())

	if s.unhealthy {
		if reason == nil {
			// node become healthy after N success
			if s.successCounter >= cfg.Monitor.Recover.SuccessCounter {
				s.unhealthy = false
				s.unhealthReportAt = time.Time{}
				monitor.ReportHealthy(s.nodeName)
			}
		} else {
			// remind long unhealthy every N minutes, even occasionally succeeded
			remindTime := s.unhealthReportAt.Add(cfg.Monitor.Recover.RemindInterval)
			if now := time.Now(); now.After(remindTime) {
				monitor.ReportUnhealthy(s.nodeName, true, reason)
				s.unhealthReportAt = now
			}
		}
	} else {
		if reason == nil {
			monitor.ReportEpoch(s.nodeName, s.latestStateEpoch)
		} else {
			// node become unhealthy
			s.unhealthy = true
			s.unhealthReportAt = time.Now()
			monitor.ReportUnhealthy(s.nodeName, false, reason)
		}
	}
}

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
	latency := time.Duration(s.latencyMetric.Snapshot().Percentile(percentile))
	if latency > maxLatency {
		return errors.Errorf("Latency too high (%v)", latency)
	}

	return nil
}

func (s *Status) Close() {
	metrics.DefaultRegistry.Unregister(s.metricName)
}
