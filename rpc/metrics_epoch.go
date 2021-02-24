package rpc

import (
	"fmt"
	"math/big"
	"sync"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/metrics"
)

var defaultEpochs = map[string]bool{
	types.EpochEarliest.String():         true,
	types.EpochLatestCheckpoint.String(): true,
	types.EpochLatestConfirmed.String():  true,
	types.EpochLatestState.String():      true,
	types.EpochLatestMined.String():      true,
}

// inputEpochMetric is used to add metrics for input epoch parameter.
type inputEpochMetric struct {
	cfx   sdk.ClientOperator
	cache map[string]map[string]string // method -> epoch -> metric
	gaps  map[string]metrics.Histogram // method -> histogram
	mutex sync.Mutex
}

func newInputEpochMetric(cfx sdk.ClientOperator) *inputEpochMetric {
	return &inputEpochMetric{
		cfx:   cfx,
		cache: make(map[string]map[string]string),
		gaps:  make(map[string]metrics.Histogram),
	}
}

func (metric *inputEpochMetric) getCacheByMethod(method string) map[string]string {
	if result, ok := metric.cache[method]; ok {
		return result
	}

	metric.mutex.Lock()
	defer metric.mutex.Unlock()

	// double check
	if result, ok := metric.cache[method]; ok {
		return result
	}

	result := make(map[string]string)
	metric.cache[method] = result

	return result
}

func (metric *inputEpochMetric) getMetricName(method string, epoch string) string {
	cache := metric.getCacheByMethod(method)
	if name, ok := cache[epoch]; ok {
		return name
	}

	metric.mutex.Lock()
	defer metric.mutex.Unlock()

	// double check
	if name, ok := cache[epoch]; ok {
		return name
	}

	name := fmt.Sprintf("rpc/input/epoch/%v/%v", method, epoch)
	cache[epoch] = name

	return name
}

func (metric *inputEpochMetric) update(epoch *types.Epoch, method string) {
	if epoch == nil {
		name := metric.getMetricName(method, "default")
		metrics.GetOrRegisterGauge(name, nil).Inc(1)
	} else if num, ok := epoch.ToInt(); ok {
		name := metric.getMetricName(method, "number")
		metrics.GetOrRegisterGauge(name, nil).Inc(1)

		if latestMined, err := metric.cfx.GetEpochNumber(types.EpochLatestMined); err == nil {
			gap := new(big.Int).Sub(latestMined.ToInt(), num)
			metric.getGapMetric(method).Update(gap.Int64())
		}
	} else {
		epochName := epoch.String()
		if _, ok := defaultEpochs[epochName]; ok {
			name := metric.getMetricName(method, epochName)
			metrics.GetOrRegisterGauge(name, nil).Inc(1)
		} else {
			name := metric.getMetricName(method, "hash")
			metrics.GetOrRegisterGauge(name, nil).Inc(1)
		}
	}
}

func (metric *inputEpochMetric) getGapMetric(method string) metrics.Histogram {
	if h, ok := metric.gaps[method]; ok {
		return h
	}

	metric.mutex.Lock()
	defer metric.mutex.Unlock()

	// double check
	if h, ok := metric.gaps[method]; ok {
		return h
	}

	name := fmt.Sprintf("rpc/input/epoch/gap/%v", method)
	h := metrics.GetOrRegisterHistogram(name, nil, metrics.NewExpDecaySample(1028, 0.015))
	metric.gaps[method] = h

	return h
}
