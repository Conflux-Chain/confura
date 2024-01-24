package metrics

import (
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

// InputEpochMetric is used to add metrics for input epoch parameter.
type InputEpochMetric struct{}

// Update marks the percentage for different epochs. If epoch number specified,
// add statistic for epoch gap against latest mined.
func (metric *InputEpochMetric) Update(epoch *types.Epoch, method string, cfx sdk.ClientOperator) {
	// mark percentage for most popular values
	Registry.RPC.InputEpoch(method, "default").Mark(epoch == nil)

	isLatestMined := types.EpochLatestMined.Equals(epoch)
	Registry.RPC.InputEpoch(method, types.EpochLatestMined.String()).Mark(isLatestMined)

	isLatestState := types.EpochLatestState.Equals(epoch)
	Registry.RPC.InputEpoch(method, types.EpochLatestState.String()).Mark(isLatestState)

	// epoch number
	var isNum bool
	if epoch != nil {
		_, isNum = epoch.ToInt()
	}
	Registry.RPC.InputEpoch(method, "number").Mark(isNum)

	// other cases
	Registry.RPC.InputEpoch(method, "others").Mark(epoch != nil && !isNum && !isLatestMined && !isLatestState)

	if epoch == nil {
		return
	}

	// update epoch gap against latest_mined if epoch number specified
	if num, ok := epoch.ToInt(); ok {
		if latestMined, err := cfx.GetEpochNumber(types.EpochLatestMined); err == nil && latestMined != nil {
			gap := latestMined.ToInt().Int64() - num.Int64()
			Registry.RPC.InputEpochGap(method).Update(gap)
		}
	}
}

func (metric *InputEpochMetric) Update2(ebh *types.EpochOrBlockHash, method string, cfx sdk.ClientOperator) {
	if ebh == nil { // default epoch
		Registry.RPC.InputBlockHash(method).Mark(false)
		metric.Update(nil, method, cfx)
		return
	}

	if _, _, ok := ebh.IsBlockHash(); ok { // block hash
		Registry.RPC.InputBlockHash(method).Mark(true)
		return
	}

	Registry.RPC.InputBlockHash(method).Mark(false)
	if epoch, ok := ebh.IsEpoch(); ok {
		metric.Update(epoch, method, cfx)
	}
}
