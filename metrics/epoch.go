package metrics

import (
	"fmt"
	"math/big"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/metrics"
)

var defaultEpochs = map[string]bool{
	types.EpochEarliest.String():         true,
	types.EpochLatestCheckpoint.String(): true,
	types.EpochLatestConfirmed.String():  true,
	types.EpochLatestState.String():      true,
	types.EpochLatestMined.String():      true,
	types.EpochLatestFinalized.String():  true,
}

// InputEpochMetric is used to add metrics for input epoch parameter.
type InputEpochMetric struct {
	cache util.ConcurrentMap // method -> epoch -> metric name
	gaps  util.ConcurrentMap // method -> histogram
}

func (metric *InputEpochMetric) Update(epoch *types.Epoch, method string, cfx sdk.ClientOperator) {
	if epoch == nil {
		name := metric.getMetricName(method, "default")
		metrics.GetOrRegisterGauge(name, nil).Inc(1)
	} else if num, ok := epoch.ToInt(); ok {
		name := metric.getMetricName(method, "number")
		metrics.GetOrRegisterGauge(name, nil).Inc(1)

		if latestMined, err := cfx.GetEpochNumber(types.EpochLatestMined); err == nil && latestMined != nil {
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

func (metric *InputEpochMetric) getMetricName(method string, epoch string) string {
	val, _ := metric.cache.LoadOrStoreFn(method, func(k interface{}) interface{} {
		// need to return pointer type for noCopy
		return &util.ConcurrentMap{}
	})

	epoch2MetricNames := val.(*util.ConcurrentMap)

	val, _ = epoch2MetricNames.LoadOrStoreFn(epoch, func(k interface{}) interface{} {
		return fmt.Sprintf("rpc/input/epoch/%v/%v", method, epoch)
	})

	return val.(string)
}

func (metric *InputEpochMetric) getGapMetric(method string) metrics.Histogram {
	val, _ := metric.gaps.LoadOrStoreFn(method, func(k interface{}) interface{} {
		return GetOrRegisterHistogram(nil, "rpc/input/epoch/gap/%v", method)
	})

	return val.(metrics.Histogram)
}
