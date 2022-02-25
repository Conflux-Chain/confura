package metrics

import (
	"fmt"
	"math/big"

	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
)

var defaultBlocks = map[rpc.BlockNumber]string{
	rpc.EarliestBlockNumber: "earliest",
	rpc.PendingBlockNumber:  "pending",
	rpc.LatestBlockNumber:   "latest",
}

// InputBlockMetric is used to add metrics for input block parameter.
type InputBlockMetric struct {
	cache util.ConcurrentMap // method -> block -> metric name
	gaps  util.ConcurrentMap // method -> histogram
}

func (metric *InputBlockMetric) Update1(blockNum *types.BlockNumber, method string, w3c *web3go.Client) {
	var blockNumOrHash *types.BlockNumberOrHash
	if blockNum != nil {
		v := types.BlockNumberOrHashWithNumber(*blockNum)
		blockNumOrHash = &v
	}

	metric.Update2(blockNumOrHash, method, w3c)
}

func (metric *InputBlockMetric) Update2(blockNumOrHash *types.BlockNumberOrHash, method string, w3c *web3go.Client) {
	switch {
	case blockNumOrHash == nil: // default
		name := metric.getMetricName(method, "default")
		metrics.GetOrRegisterGauge(name, nil).Inc(1)
	case blockNumOrHash.BlockHash != nil: // block hash
		name := metric.getMetricName(method, "hash")
		metrics.GetOrRegisterGauge(name, nil).Inc(1)
	case blockNumOrHash.BlockNumber != nil: // block number
		blockNum := *blockNumOrHash.BlockNumber
		if blockName, ok := defaultBlocks[rpc.BlockNumber(blockNum)]; ok { // named block?
			name := metric.getMetricName(method, blockName)
			metrics.GetOrRegisterGauge(name, nil).Inc(1)
			break
		}

		name := metric.getMetricName(method, "number")
		metrics.GetOrRegisterGauge(name, nil).Inc(1)

		if latestBlockNum, err := w3c.Eth.BlockNumber(); err == nil { // also metric block gap
			gap := new(big.Int).Sub(latestBlockNum, big.NewInt(int64(blockNum)))
			metric.getGapMetric(method).Update(gap.Int64())
		}
	}
}

func (metric *InputBlockMetric) getMetricName(method string, block string) string {
	val, _ := metric.cache.LoadOrStoreFn(method, func(k interface{}) interface{} {
		// need to return pointer type for noCopy
		return &util.ConcurrentMap{}
	})

	block2MetricNames := val.(*util.ConcurrentMap)

	val, _ = block2MetricNames.LoadOrStoreFn(block, func(k interface{}) interface{} {
		return fmt.Sprintf("rpc/input/block/%v/%v", method, block)
	})

	return val.(string)
}

func (metric *InputBlockMetric) getGapMetric(method string) metrics.Histogram {
	val, _ := metric.gaps.LoadOrStoreFn(method, func(k interface{}) interface{} {
		return GetOrRegisterHistogram(nil, "rpc/input/block/gap/%v", method)
	})

	return val.(metrics.Histogram)
}
