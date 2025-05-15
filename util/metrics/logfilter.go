package metrics

import (
	"math/big"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	w3types "github.com/openweb3/web3go/types"
)

type EthNodeMetricsReader interface {
	BlockNumber() (*big.Int, error)
}

func UpdateEthRpcLogFilter(method string, eth EthNodeMetricsReader, filter *w3types.FilterQuery) {
	Registry.RPC.Percentage(method, "filter/hash").Mark(filter.BlockHash != nil)
	Registry.RPC.Percentage(method, "filter/address/null").Mark(len(filter.Addresses) == 0)
	Registry.RPC.Percentage(method, "address/single").Mark(len(filter.Addresses) == 1)
	Registry.RPC.Percentage(method, "address/multiple").Mark(len(filter.Addresses) > 1)
	Registry.RPC.Percentage(method, "filter/topics").Mark(len(filter.Topics) > 0)

	if filter.BlockHash == nil {
		m := InputBlockMetric{}
		m.Update1(filter.FromBlock, method+"/from", eth)
		m.Update1(filter.ToBlock, method+"/to", eth)
	}
}

func UpdateCfxRpcLogFilter(method string, cfx sdk.ClientOperator, filter *types.LogFilter) {
	isBlockRange := filter.FromBlock != nil || filter.ToBlock != nil
	isBlockHashes := len(filter.BlockHashes) > 0
	isEpochRange := !isBlockRange && !isBlockHashes
	Registry.RPC.Percentage(method, "filter/epochRange").Mark(isEpochRange)
	Registry.RPC.Percentage(method, "filter/blockRange").Mark(isBlockRange)
	Registry.RPC.Percentage(method, "filter/hashes").Mark(isBlockHashes)
	Registry.RPC.Percentage(method, "filter/address/null").Mark(len(filter.Address) == 0)
	Registry.RPC.Percentage(method, "filter/address/single").Mark(len(filter.Address) == 1)
	Registry.RPC.Percentage(method, "filter/address/multiple").Mark(len(filter.Address) > 1)
	Registry.RPC.Percentage(method, "filter/topics").Mark(len(filter.Topics) > 0)

	// add metrics for the `epoch` filter only if block hash and block number range are not specified.
	if len(filter.BlockHashes) == 0 && filter.FromBlock == nil && filter.ToBlock == nil {
		m := InputEpochMetric{}
		m.Update(filter.FromEpoch, method+"/from", cfx)
		m.Update(filter.ToEpoch, method+"/to", cfx)
	}
}
