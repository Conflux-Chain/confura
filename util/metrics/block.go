package metrics

import (
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
)

// InputBlockMetric is used to add metrics for input block parameter.
type InputBlockMetric struct{}

func (metric *InputBlockMetric) updateBlockNumberIgnoreDefault(blockNum *types.BlockNumber, method string, w3c *web3go.Client) {
	// mark percentage for most popular values
	Registry.RPC.InputBlock(method, "latest").Mark(blockNum != nil && *blockNum == rpc.LatestBlockNumber)
	Registry.RPC.InputBlock(method, "pending").Mark(blockNum != nil && *blockNum == rpc.PendingBlockNumber)
	// metric.updatePercentage(method, "earliest", blockNum != nil && *blockNum == rpc.EarliestBlockNumber)

	// block number
	isNum := blockNum != nil && *blockNum > 0
	Registry.RPC.InputBlock(method, "number").Mark(isNum)

	if !isNum {
		return
	}

	// update block gap against latest if block number specified
	if latestBlockNum, err := w3c.Eth.BlockNumber(); err == nil && latestBlockNum != nil {
		gap := latestBlockNum.Int64() - int64(*blockNum)
		Registry.RPC.InputBlockGap(method).Update(gap)
	}
}

func (metric *InputBlockMetric) Update1(blockNum *types.BlockNumber, method string, w3c *web3go.Client) {
	Registry.RPC.InputBlock(method, "default").Mark(blockNum == nil)
	metric.updateBlockNumberIgnoreDefault(blockNum, method, w3c)
}

func (metric *InputBlockMetric) Update2(blockNumOrHash *types.BlockNumberOrHash, method string, w3c *web3go.Client) {
	Registry.RPC.InputBlock(method, "default").Mark(blockNumOrHash == nil)
	Registry.RPC.InputBlock(method, "hash").Mark(blockNumOrHash != nil && blockNumOrHash.BlockHash != nil)

	var blockNum *types.BlockNumber
	if blockNumOrHash != nil {
		blockNum = blockNumOrHash.BlockNumber
	}
	metric.updateBlockNumberIgnoreDefault(blockNum, method, w3c)
}
