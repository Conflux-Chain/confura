package metrics

import (
	"fmt"
	"time"

	"github.com/Conflux-Chain/confura/util"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/openweb3/go-rpc-provider/utils"
)

var Registry Metrics

type Metrics struct {
	RPC           RpcMetrics
	PubSub        PubSubMetrics
	Sync          SyncMetrics
	Store         StoreMetrics
	Nodes         NodeManagerMetrics
	VirtualFilter VirtualFilterMetrics
}

// RPC metrics
type RpcMetrics struct{}

// RPC metrics - proxy
// RPC server integrated metrics:
// rpc/requests.gauge, rpc/success.gauge, rpc/failure.gauge
// rpc/duration/$method/success.timer
// rpc/duration/$method/failure.timer

func (*RpcMetrics) BatchSize() metrics.Histogram {
	return GetOrRegisterHistogram("infura/rpc/batch/size")
}

func (*RpcMetrics) BatchLatency() metrics.Histogram {
	return GetOrRegisterHistogram("infura/rpc/batch/latency")
}

func (*RpcMetrics) UpdateDuration(method string, err error, start time.Time) {
	var isNilErr, isRpcErr bool
	if isNilErr = util.IsInterfaceValNil(err); !isNilErr {
		isRpcErr = utils.IsRPCJSONError(err)
	}

	// Overall rate statistics
	GetOrRegisterTimeWindowPercentageDefault("infura/rpc/rate/success").Mark(isNilErr)
	GetOrRegisterTimeWindowPercentageDefault("infura/rpc/rate/rpcErr").Mark(isRpcErr)
	GetOrRegisterTimeWindowPercentageDefault("infura/rpc/rate/nonRpcErr").Mark(!isNilErr && !isRpcErr)

	// RPC rate statistics
	GetOrRegisterTimeWindowPercentageDefault("infura/rpc/rate/success/%v", method).Mark(isNilErr)
	GetOrRegisterTimeWindowPercentageDefault("infura/rpc/rate/rpcErr/%v", method).Mark(isRpcErr)
	GetOrRegisterTimeWindowPercentageDefault("infura/rpc/rate/nonRpcErr/%v", method).Mark(!isNilErr && !isRpcErr)

	// Only update QPS & Latency if success or rpc error. Because, io error usually takes long time
	// and impact the average latency.
	if isNilErr || isRpcErr {
		GetOrRegisterTimer("infura/rpc/duration/all").UpdateSince(start)
		GetOrRegisterTimer("infura/rpc/duration/%v", method).UpdateSince(start)
	}
}

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
	if util.IsInterfaceValNil(err) {
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

func (*SyncMetrics) SyncOnceQps(space, storeName string, err error) metrics.Timer {
	if util.IsInterfaceValNil(err) {
		return GetOrRegisterTimer("infura/sync/%v/%v/once/success", space, storeName)
	}

	return GetOrRegisterTimer("infura/sync/%v/%v/once/failure", space, storeName)
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

// PubSub metrics
type PubSubMetrics struct{}

func (*PubSubMetrics) Sessions(space, topic, node string) metrics.Gauge {
	return GetOrRegisterGauge("infura/pubsub/%v/sessions/%v/%v", space, topic, node)
}

func (*PubSubMetrics) InputLogFilter(space string) Percentage {
	return GetOrRegisterTimeWindowPercentageDefault("infura/pubsub/%v/input/logFilter", space)
}

// Virtual filter metrics
type VirtualFilterMetrics struct{}

func (*VirtualFilterMetrics) Sessions(filterType, node string) metrics.Gauge {
	return GetOrRegisterGauge("infura/virtualFilter/%v/sessions/%v", filterType, node)
}

func (*VirtualFilterMetrics) PollOnceQps(node string, err error) metrics.Timer {
	if util.IsInterfaceValNil(err) {
		return GetOrRegisterTimer("infura/virtualFilter/poll/%v/once/success", node)
	}

	return GetOrRegisterTimer("infura/virtualFilter/poll/%v/once/failure", node)
}

func (*VirtualFilterMetrics) PollOnceSize(node string) metrics.Histogram {
	return GetOrRegisterHistogram("infura/virtualFilter/poll/%v/once/size", node)
}

func (*VirtualFilterMetrics) PersistFilterChanges(node, store string) TimerUpdater {
	return NewTimerUpdaterByName(fmt.Sprintf("infura/virtualFilter/persist/%v/filterChanges/%v", node, store))
}

func (*VirtualFilterMetrics) QueryFilterChanges(node, store string) TimerUpdater {
	metricName := fmt.Sprintf("infura/virtualFilter/query/%v/filterChanges/%v", node, store)
	return NewTimerUpdaterByName(metricName)
}

func (*VirtualFilterMetrics) StoreQueryPercentage(node, store string) Percentage {
	metricName := fmt.Sprintf("infura/virtualFilter/percentage/query/%v/filterChanges/%v", node, store)
	return GetOrRegisterTimeWindowPercentageDefault(metricName)
}
