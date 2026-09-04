package rpc

import (
	"testing"

	"github.com/Conflux-Chain/confura/node"
	"github.com/stretchr/testify/assert"
)

func TestScanLogsMethodsUseDedicatedNodeGroups(t *testing.T) {
	ethCases := map[string]node.Group{
		"eth_scanLogs":                    node.GroupEthLogs,
		"eth_scanLogsWithPivotAssumption": node.GroupEthLogs,
		"eth_getLogs":                     node.GroupEthLogs,
		"eth_blockNumber":                 node.GroupEthHttp,
	}
	for method, want := range ethCases {
		t.Run(method, func(t *testing.T) {
			assert.Equal(t, want, ethClientGroupForMethod(method))
		})
	}

	cfxCases := map[string]node.Group{
		"cfx_scanLogs":                    node.GroupCfxLogs,
		"cfx_scanLogsWithPivotAssumption": node.GroupCfxLogs,
		"cfx_getLogs":                     node.GroupCfxLogs,
		"cfx_epochNumber":                 node.GroupCfxHttp,
	}
	for method, want := range cfxCases {
		t.Run(method, func(t *testing.T) {
			assert.Equal(t, want, cfxClientGroupForMethod(method))
		})
	}
}
