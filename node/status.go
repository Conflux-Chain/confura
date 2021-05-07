package node

import (
	"fmt"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	infuraMetrics "github.com/conflux-chain/conflux-infura/metrics"
	"github.com/ethereum/go-ethereum/metrics"
)

type Status struct {
	metricName       string
	latencyMetric    metrics.Histogram // ping latency via cfx_epochNumber
	latestStateEpoch uint64
	failureCounter   uint64
}

func NewStatus(nodeName string) Status {
	metricName := fmt.Sprintf("infura/nodes/latency/%v", nodeName)

	return Status{
		metricName:    metricName,
		latencyMetric: infuraMetrics.GetOrRegisterHistogram(nil, metricName),
	}
}

func (s *Status) LatestStateEpoch() uint64         { return s.latestStateEpoch }
func (s *Status) FailureCounter() uint64           { return s.failureCounter }
func (s *Status) LatencyMetric() metrics.Histogram { return s.latencyMetric }

func (s *Status) Update(cfx sdk.ClientOperator) {
	start := time.Now()
	epoch, err := cfx.GetEpochNumber(types.EpochLatestState)
	if err != nil {
		s.failureCounter++
	} else {
		s.latencyMetric.Update(time.Since(start).Nanoseconds())
		s.latestStateEpoch = epoch.ToInt().Uint64()
		s.failureCounter = 0
	}
}

func (s *Status) Close() {
	metrics.DefaultRegistry.Unregister(s.metricName)
}
