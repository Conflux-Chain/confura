package metrics

import (
	"github.com/openweb3/web3go/client"
	"github.com/openweb3/web3go/types"
)

// InputBlockMetric is used to add metrics for input block parameter.
type InputBlockMetric struct{}

func (metric *InputBlockMetric) updateBlockNumberIgnoreDefault(blockNum *types.BlockNumber, method string, eth *client.RpcEthClient) {
	// mark percentage for most popular values
	isLatest := (blockNum != nil && *blockNum == types.LatestBlockNumber)
	Registry.RPC.InputBlock(method, "latest").Mark(isLatest)

	isPending := (blockNum != nil && *blockNum == types.PendingBlockNumber)
	Registry.RPC.InputBlock(method, "pending").Mark(isPending)

	// block number
	isNum := blockNum != nil && *blockNum > 0
	Registry.RPC.InputBlock(method, "number").Mark(isNum)

	// other cases
	Registry.RPC.InputBlock(method, "others").Mark(blockNum != nil && !isNum && !isLatest && !isPending)

	if !isNum {
		return
	}

	// update block gap against latest if block number specified
	if latestBlockNum, err := eth.BlockNumber(); err == nil && latestBlockNum != nil {
		gap := latestBlockNum.Int64() - int64(*blockNum)
		Registry.RPC.InputBlockGap(method).Update(gap)
	}
}

func (metric *InputBlockMetric) Update1(blockNum *types.BlockNumber, method string, eth *client.RpcEthClient) {
	Registry.RPC.InputBlock(method, "default").Mark(blockNum == nil)
	metric.updateBlockNumberIgnoreDefault(blockNum, method, eth)
}

func (metric *InputBlockMetric) Update2(blockNumOrHash *types.BlockNumberOrHash, method string, eth *client.RpcEthClient) {
	Registry.RPC.InputBlock(method, "default").Mark(blockNumOrHash == nil)
	Registry.RPC.InputBlock(method, "hash").Mark(blockNumOrHash != nil && blockNumOrHash.BlockHash != nil)

	var blockNum *types.BlockNumber
	if blockNumOrHash != nil {
		blockNum = blockNumOrHash.BlockNumber
	}

	metric.updateBlockNumberIgnoreDefault(blockNum, method, eth)
}
