package node

import (
	"fmt"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	infuraMetrics "github.com/conflux-chain/conflux-infura/metrics"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type HealthMonitor interface {
	HealthyEpoch() uint64
	ReportEpoch(nodeName string, epoch uint64)
	ReportUnhealthy(nodeName string, remind bool, reason error)
	ReportHealthy(nodeName string)
}

type Status struct {
	nodeName         string
	metricName       string
	latencyMetric    metrics.Histogram // ping latency via cfx_epochNumber
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
			recoverThreshold := viper.GetUint64("node.monitor.recover.successCounter")
			if s.successCounter >= recoverThreshold {
				s.unhealthy = false
				s.unhealthReportAt = time.Time{}
				monitor.ReportHealthy(s.nodeName)
			}
		} else {
			// remind long unhealthy every N minutes, even occasionally suscceeded
			remindInterval := viper.GetDuration("node.monitor.recover.remindInterval")
			remindTime := s.unhealthReportAt.Add(remindInterval)
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
	failureThreshold := viper.GetUint64("node.monitor.unhealth.failures")
	if s.failureCounter >= failureThreshold {
		return errors.Errorf("RPC failures (%v)", s.failureCounter)
	}

	// epoch fall behind
	numEpochsFallBehind := viper.GetUint64("node.monitor.unhealth.epochsFallBehind")
	if s.latestStateEpoch+numEpochsFallBehind < targetEpoch {
		return errors.Errorf("Epoch fall behind (%v)", targetEpoch-s.latestStateEpoch)
	}

	// latency too high
	percentile := viper.GetFloat64("node.monitor.unhealth.latencyPercentile")
	maxLatency := viper.GetDuration("node.monitor.unhealth.maxLatency")
	latency := time.Duration(s.latencyMetric.Snapshot().Percentile(percentile))
	if latency > maxLatency {
		return errors.Errorf("Latency too high (%v)", latency)
	}

	return nil
}

func (s *Status) Close() {
	metrics.DefaultRegistry.Unregister(s.metricName)
}
