package metrics

import (
	"fmt"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/metrics"
)

// InputEpochMetric is used to add metrics for input epoch parameter.
type InputEpochMetric struct {
	cache util.ConcurrentMap // method -> epoch -> metric name
	gaps  util.ConcurrentMap // method -> histogram
}

// Update marks the percentage for different epochs. If epoch number specified,
// add statistic for epoch gap against latest mined.
func (metric *InputEpochMetric) Update(epoch *types.Epoch, method string, cfx sdk.ClientOperator) {
	// mark percentage for most popular values
	metric.updatePercentage(method, "default", epoch == nil)
	metric.updatePercentage(method, types.EpochLatestMined.String(), types.EpochLatestMined.Equals(epoch))
	metric.updatePercentage(method, types.EpochLatestState.String(), types.EpochLatestState.Equals(epoch))

	// epoch number
	var isNum bool
	if epoch != nil {
		_, isNum = epoch.ToInt()
	}
	metric.updatePercentage(method, "number", isNum)

	// other cases
	metric.updatePercentage(method, "others",
		epoch != nil && !isNum &&
			!types.EpochLatestMined.Equals(epoch) &&
			!types.EpochLatestState.Equals(epoch))

	if epoch == nil {
		return
	}

	// update epoch gap against latest_mined if epoch number specified
	if num, ok := epoch.ToInt(); ok {
		if latestMined, err := cfx.GetEpochNumber(types.EpochLatestMined); err == nil && latestMined != nil {
			gap := latestMined.ToInt().Int64() - num.Int64()
			metric.updateGap(method, gap)
		}
	}
}

func (metric *InputEpochMetric) updatePercentage(method string, epoch string, hit bool) {
	val, _ := metric.cache.LoadOrStoreFn(method, func(interface{}) interface{} {
		// need to return pointer type for noCopy
		return &util.ConcurrentMap{}
	})

	epoch2MetricNames := val.(*util.ConcurrentMap)

	val, _ = epoch2MetricNames.LoadOrStoreFn(epoch, func(interface{}) interface{} {
		return fmt.Sprintf("rpc/input/epoch/%v/%v", method, epoch)
	})

	GetOrRegisterTimeWindowPercentageDefault(val.(string)).Mark(hit)
}

func (metric *InputEpochMetric) updateGap(method string, gap int64) {
	val, _ := metric.gaps.LoadOrStoreFn(method, func(interface{}) interface{} {
		return GetOrRegisterHistogram("rpc/input/epoch/gap/%v", method)
	})

	val.(metrics.Histogram).Update(gap)
}
