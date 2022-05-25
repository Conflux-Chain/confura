package metrics

import (
	"fmt"

	"github.com/ethereum/go-ethereum/metrics"
)

var Registry Metrics

type Metrics struct {
	RPC   RpcMetrics
	Sync  SyncMetrics
	Store StoreMetrics
	Nodes NodeManagerMetrics
}

// RPC metrics
type RpcMetrics struct{}

// RPC metrics - proxy
// RPC server integrated metrics:
// rpc/requests.gauge, rpc/success.gauge, rpc/failure.gauge
// rpc/duration/$method/success.timer
// rpc/duration/$method/failure.timer

// RPC metrics - inputs

func (*RpcMetrics) InputEpoch(method, epoch string) Percentage {
	return GetOrRegisterTimeWindowPercentageDefault("infura/rpc/input/epoch/%v/%v", method, epoch)
}

func (*RpcMetrics) InputEpochGap(method string) metrics.Histogram {
	return GetOrRegisterHistogram("infura/rpc/input/epoch/gap/%v", method)
}

func (*RpcMetrics) InputBlock(method, block string) Percentage {
	return GetOrRegisterTimeWindowPercentageDefault("infura/rpc/input/block/%v/%v", method, block)
}

func (*RpcMetrics) InputBlockGap(method string) metrics.Histogram {
	return GetOrRegisterHistogram("infura/rpc/input/block/gap/%v", method)
}

// PRC metrics - percentages

func (*RpcMetrics) Percentage(method, name string) Percentage {
	return GetOrRegisterTimeWindowPercentageDefault("infura/rpc/percentage/%v/%v", method, name)
}

// RPC metrics - store hit ratio

func (*RpcMetrics) StoreHit(method, storeName string) Percentage {
	// use rpc method to distinguish cfx and eth
	return GetOrRegisterTimeWindowPercentageDefault("infura/rpc/store/hit/%v/%v", storeName, method)
}

// RPC metrics - fullnode

func (*RpcMetrics) FullnodeQps(space, method string, err error) metrics.Timer {
	if err == nil {
		return GetOrRegisterTimer("infura/rpc/fullnode/%v/%v/success", space, method)
	}

	return GetOrRegisterTimer("infura/rpc/fullnode/%v/%v/failure", space, method)
}

func (*RpcMetrics) FullnodeErrorRate(node ...string) Percentage {
	if len(node) == 0 {
		return GetOrRegisterTimeWindowPercentageDefault("infura/rpc/fullnode/rate/error")
	}

	return GetOrRegisterTimeWindowPercentageDefault("infura/rpc/fullnode/rate/error/%v", node[0])
}

func (*RpcMetrics) FullnodeNonRpcErrorRate(node ...string) Percentage {
	if len(node) == 0 {
		return GetOrRegisterTimeWindowPercentageDefault("infura/rpc/fullnode/rate/nonRpcErr")
	}

	return GetOrRegisterTimeWindowPercentageDefault("infura/rpc/fullnode/rate/nonRpcErr/%v", node[0])
}

// Sync service metrics
type SyncMetrics struct{}

func (*SyncMetrics) SyncOnceQps(space, storeName string) TimerUpdater {
	return NewTimerUpdaterByName(fmt.Sprintf("infura/sync/%v/%v/once", space, storeName))
}

func (*SyncMetrics) SyncOnceSize(space, storeName string) metrics.Histogram {
	return GetOrRegisterHistogram("infura/sync/%v/%v/once/size", space, storeName)
}

func (*SyncMetrics) QueryEpochData(space string) TimerUpdater {
	return NewTimerUpdaterByName(fmt.Sprintf("infura/sync/%v/fullnode", space))
}

func (*SyncMetrics) QueryEpochDataAvailability(space string) Percentage {
	return GetOrRegisterTimeWindowPercentageDefault("infura/sync/%v/fullnode/availability", space)
}

// Store metrics
type StoreMetrics struct{}

func (*StoreMetrics) Push(storeName string) TimerUpdater {
	return NewTimerUpdaterByName(fmt.Sprintf("infura/store/%v/push", storeName))
}

func (*StoreMetrics) Pop(storeName string) TimerUpdater {
	return NewTimerUpdaterByName(fmt.Sprintf("infura/store/%v/pop", storeName))
}

func (*StoreMetrics) GetLogs() TimerUpdater {
	return NewTimerUpdaterByName("infura/store/mysql/getlogs")
}

// Node manager metrics
type NodeManagerMetrics struct{}

func (*NodeManagerMetrics) Routes(space, group, node string) metrics.Meter {
	return GetOrRegisterMeter("infura/nodes/%v/routes/%v/%v", space, group, node)
}

func (*NodeManagerMetrics) NodeLatency(space, group, node string) string {
	return fmt.Sprintf("infura/nodes/%v/latency/%v/%v", space, group, node)
}

func (*NodeManagerMetrics) NodeAvailability(space, group, node string) string {
	return fmt.Sprintf("infura/nodes/%v/availability/%v/%v", space, group, node)
}
