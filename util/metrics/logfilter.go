package metrics

import (
	"github.com/openweb3/web3go/client"
	"github.com/openweb3/web3go/types"
)

func UpdateEthRpcLogFilter(method string, eth *client.RpcEthClient, filter *types.FilterQuery) {
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
