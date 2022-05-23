package metrics

import (
	"fmt"

	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
)

// InputBlockMetric is used to add metrics for input block parameter.
type InputBlockMetric struct {
	cache util.ConcurrentMap // method -> block -> metric name
	gaps  util.ConcurrentMap // method -> histogram
}

func (metric *InputBlockMetric) updateBlockNumberIgnoreDefault(blockNum *types.BlockNumber, method string, w3c *web3go.Client) {
	// mark percentage for most popular values
	metric.updatePercentage(method, "latest", blockNum != nil && *blockNum == rpc.LatestBlockNumber)
	metric.updatePercentage(method, "pending", blockNum != nil && *blockNum == rpc.PendingBlockNumber)
	// metric.updatePercentage(method, "earliest", blockNum != nil && *blockNum == rpc.EarliestBlockNumber)

	// block number
	isNum := blockNum != nil && *blockNum > 0
	metric.updatePercentage(method, "number", isNum)

	if !isNum {
		return
	}

	// update block gap against latest if block number specified
	if latestBlockNum, err := w3c.Eth.BlockNumber(); err == nil && latestBlockNum != nil {
		gap := latestBlockNum.Int64() - int64(*blockNum)
		metric.updateGap(method, gap)
	}
}

func (metric *InputBlockMetric) Update1(blockNum *types.BlockNumber, method string, w3c *web3go.Client) {
	metric.updatePercentage(method, "default", blockNum == nil)
	metric.updateBlockNumberIgnoreDefault(blockNum, method, w3c)
}

func (metric *InputBlockMetric) Update2(blockNumOrHash *types.BlockNumberOrHash, method string, w3c *web3go.Client) {
	metric.updatePercentage(method, "default", blockNumOrHash == nil)
	metric.updatePercentage(method, "hash", blockNumOrHash != nil && blockNumOrHash.BlockHash != nil)

	var blockNum *types.BlockNumber
	if blockNumOrHash != nil {
		blockNum = blockNumOrHash.BlockNumber
	}
	metric.updateBlockNumberIgnoreDefault(blockNum, method, w3c)
}

func (metric *InputBlockMetric) updatePercentage(method string, block string, hit bool) {
	val, _ := metric.cache.LoadOrStoreFn(method, func(interface{}) interface{} {
		// need to return pointer type for noCopy
		return &util.ConcurrentMap{}
	})

	block2MetricNames := val.(*util.ConcurrentMap)

	val, _ = block2MetricNames.LoadOrStoreFn(block, func(interface{}) interface{} {
		return fmt.Sprintf("rpc/input/block/%v/%v", method, block)
	})

	GetOrRegisterTimeWindowPercentageDefault(val.(string)).Mark(hit)
}

func (metric *InputBlockMetric) updateGap(method string, gap int64) {
	val, _ := metric.gaps.LoadOrStoreFn(method, func(interface{}) interface{} {
		return GetOrRegisterHistogram("rpc/input/block/gap/%v", method)
	})

	val.(metrics.Histogram).Update(gap)
}
