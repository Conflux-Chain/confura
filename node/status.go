package node

import (
	"fmt"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	infuraMetrics "github.com/conflux-chain/conflux-infura/metrics"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/pkg/errors"
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
	latencyMetric metrics.Histogram // ping latency via cfx_epochNumber

	latestStateEpoch uint64
	successCounter   uint64
	failureCounter   uint64
	unhealthy        bool
	unhealthReportAt time.Time
}

func NewStatus(nodeName string) Status {
	metricName := fmt.Sprintf("infura/nodes/latency/%v", nodeName)

	return Status{
		nodeName:      nodeName,
		metricName:    metricName,
		latencyMetric: infuraMetrics.GetOrRegisterHistogram(nil, metricName),
	}
}

func (s *Status) Update(cfx sdk.ClientOperator, monitor HealthMonitor) {
	s.heartbeat(cfx)
	s.updateHealth(monitor)
}

func (s *Status) heartbeat(cfx sdk.ClientOperator) {
	start := time.Now()
	epoch, err := cfx.GetEpochNumber(types.EpochLatestState)
	if err != nil {
		s.failureCounter++
		s.successCounter = 0
	} else {
		s.latencyMetric.Update(time.Since(start).Nanoseconds())
		s.latestStateEpoch = epoch.ToInt().Uint64()
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
