package metrics

import (
	"fmt"
	"time"

	"github.com/Conflux-Chain/confura/util"
	metricUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
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
	Client        ClientMetrics
}

// RPC metrics
type RpcMetrics struct{}

// RPC metrics - proxy
// RPC server integrated metrics:
// rpc/requests.gauge, rpc/success.gauge, rpc/failure.gauge
// rpc/duration/$method/success.timer
// rpc/duration/$method/failure.timer

func (*RpcMetrics) BatchSize() metrics.Histogram {
	return metricUtil.GetOrRegisterHistogram("infura/rpc/batch/size")
}

func (*RpcMetrics) BatchLatency() metrics.Histogram {
	return metricUtil.GetOrRegisterHistogram("infura/rpc/batch/latency")
}

func (*RpcMetrics) UpdateDuration(space, method string, err error, start time.Time) {
	var isNilErr, isRpcErr bool
	if isNilErr = util.IsInterfaceValNil(err); !isNilErr {
		isRpcErr = utils.IsRPCJSONError(err)
	}

	// Overall rate statistics
	metricUtil.GetOrRegisterTimeWindowPercentageDefault(100, "infura/rpc/rate/success/%v", space).Mark(isNilErr)
	metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/rate/rpcErr/%v", space).Mark(isRpcErr)
	metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/rate/nonRpcErr/%v", space).Mark(!isNilErr && !isRpcErr)

	// RPC rate statistics
	metricUtil.GetOrRegisterTimeWindowPercentageDefault(100, "infura/rpc/rate/success/%v/%v", space, method).Mark(isNilErr)
	metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/rate/rpcErr/%v/%v", space, method).Mark(isRpcErr)
	metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/rate/nonRpcErr/%v/%v", space, method).Mark(!isNilErr && !isRpcErr)

	// Only update QPS & Latency if success or rpc error. Because, io error usually takes long time
	// and impact the average latency.
	if isNilErr || isRpcErr {
		metricUtil.GetOrRegisterTimer("infura/rpc/duration/all/%v", space).UpdateSince(start)
		metricUtil.GetOrRegisterTimer("infura/rpc/duration/%v/%v", space, method).UpdateSince(start)
	}
}

func (*RpcMetrics) ResponseSize(space, method string) metrics.Histogram {
	return metricUtil.GetOrRegisterHistogram("infura/rpc/response/size/%v/%v", space, method)
}

// RPC metrics - inputs

func (*RpcMetrics) InputEpoch(method, epoch string) metricUtil.Percentage {
	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/input/epoch/%v/%v", method, epoch)
}

func (*RpcMetrics) InputBlockHash(method string) metricUtil.Percentage {
	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/input/blockHash/%v", method)
}

func (*RpcMetrics) InputEpochGap(method string) metrics.Histogram {
	return metricUtil.GetOrRegisterHistogram("infura/rpc/input/epoch/gap/%v", method)
}

func (*RpcMetrics) InputBlock(method, block string) metricUtil.Percentage {
	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/input/block/%v/%v", method, block)
}

func (*RpcMetrics) InputBlockGap(method string) metrics.Histogram {
	return metricUtil.GetOrRegisterHistogram("infura/rpc/input/block/gap/%v", method)
}

// RPC metrics - handler

func (*RpcMetrics) LogFilterSplit(method, name string) metrics.Histogram {
	return metricUtil.GetOrRegisterHistogram("infura/rpc/handler/%v/filter/split/%v", method, name)
}

// PRC metrics - percentages

func (*RpcMetrics) Percentage(method, name string) metricUtil.Percentage {
	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/percentage/%v/%v", method, name)
}

// RPC metrics - store hit ratio

func (*RpcMetrics) StoreHit(method, storeName string) metricUtil.Percentage {
	// use rpc method to distinguish cfx and eth
	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/store/hit/%v/%v", storeName, method)
}

// RPC metrics - fullnode

func (*RpcMetrics) FullnodeQps(node, space, method string, err error) metrics.Timer {
	if util.IsInterfaceValNil(err) {
		return metricUtil.GetOrRegisterTimer("infura/rpc/fullnode/%v/%v/%v/success", node, space, method)
	}

	return metricUtil.GetOrRegisterTimer("infura/rpc/fullnode/%v/%v/%v/failure", node, space, method)
}

func (*RpcMetrics) FullnodeErrorRate(node ...string) metricUtil.Percentage {
	if len(node) == 0 {
		return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/fullnode/rate/error")
	}

	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/fullnode/rate/error/%v", node[0])
}

func (*RpcMetrics) FullnodeNonRpcErrorRate(node ...string) metricUtil.Percentage {
	if len(node) == 0 {
		return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/fullnode/rate/nonRpcErr")
	}

	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/rpc/fullnode/rate/nonRpcErr/%v", node[0])
}

// Sync service metrics
type SyncMetrics struct{}

func (*SyncMetrics) SyncOnceQps(space, storeName string, err error) metrics.Timer {
	if util.IsInterfaceValNil(err) {
		return metricUtil.GetOrRegisterTimer("infura/sync/%v/%v/once/success", space, storeName)
	}

	return metricUtil.GetOrRegisterTimer("infura/sync/%v/%v/once/failure", space, storeName)
}

func (*SyncMetrics) SyncOnceSize(space, storeName string) metrics.Histogram {
	return metricUtil.GetOrRegisterHistogram("infura/sync/%v/%v/once/size", space, storeName)
}

func (*SyncMetrics) QueryEpochData(space string) metrics.Timer {
	return metricUtil.GetOrRegisterTimer("infura/sync/%v/fullnode", space)
}

func (*SyncMetrics) BoostQueryEpochData(space string) metrics.Timer {
	return metricUtil.GetOrRegisterTimer("infura/sync/boost/%v/fullnode", space)
}

func (*SyncMetrics) BoostQueryEpochRange() metrics.Histogram {
	return metricUtil.GetOrRegisterHistogram("infura/sync/boost/epoch/range")
}

func (*SyncMetrics) QueryEpochDataAvailability(space string) metricUtil.Percentage {
	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/sync/%v/fullnode/availability", space)
}

func (*SyncMetrics) BoostQueryEpochDataAvailability(space string) metricUtil.Percentage {
	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/sync/boost/%v/fullnode/availability", space)
}

// Store metrics
type StoreMetrics struct{}

func (*StoreMetrics) Push(storeName string) metrics.Timer {
	return metricUtil.GetOrRegisterTimer("infura/store/%v/push", storeName)
}

func (*StoreMetrics) Pop(storeName string) metrics.Timer {
	return metricUtil.GetOrRegisterTimer("infura/store/%v/pop", storeName)
}

func (*StoreMetrics) GetLogs() metrics.Timer {
	return metricUtil.GetOrRegisterTimer("infura/store/mysql/getlogs")
}

// Node manager metrics
type NodeManagerMetrics struct{}

func (*NodeManagerMetrics) Routes(space, group, node string) metrics.Meter {
	return metricUtil.GetOrRegisterMeter("infura/nodes/%v/routes/%v/%v", space, group, node)
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
	return metricUtil.GetOrRegisterGauge("infura/pubsub/%v/sessions/%v/%v", space, topic, node)
}

func (*PubSubMetrics) InputLogFilter(space string) metricUtil.Percentage {
	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/pubsub/%v/input/logFilter", space)
}

// Virtual filter metrics
type VirtualFilterMetrics struct{}

func (*VirtualFilterMetrics) Sessions(space string, filterType, node string) metrics.Gauge {
	return metricUtil.GetOrRegisterGauge("infura/virtualFilter/%v/%v/sessions/%v", space, filterType, node)
}

func (*VirtualFilterMetrics) PollOnceQps(space, node string, err error) metrics.Timer {
	if util.IsInterfaceValNil(err) {
		return metricUtil.GetOrRegisterTimer("infura/virtualFilter/%v/poll/%v/once/success", space, node)
	}

	return metricUtil.GetOrRegisterTimer("infura/virtualFilter/%v/poll/%v/once/failure", space, node)
}

func (*VirtualFilterMetrics) PollOnceSize(space, node string) metrics.Histogram {
	return metricUtil.GetOrRegisterHistogram("infura/virtualFilter/%v/poll/%v/once/size", space, node)
}

func (*VirtualFilterMetrics) PersistFilterChanges(space, node, store string) metrics.Timer {
	return metricUtil.GetOrRegisterTimer("infura/virtualFilter/%v/persist/%v/filterChanges/%v", space, node, store)
}

func (*VirtualFilterMetrics) QueryFilterChanges(space, node, store string) metrics.Timer {
	return metricUtil.GetOrRegisterTimer("infura/virtualFilter/%v/query/%v/filterChanges/%v", space, node, store)
}

func (*VirtualFilterMetrics) StoreQueryPercentage(space string, node, store string) metricUtil.Percentage {
	metricName := fmt.Sprintf("infura/virtualFilter/%v/percentage/query/%v/filterChanges/%v", space, node, store)
	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, metricName)
}

// Client metrics

type ClientMetrics struct{}

func (*ClientMetrics) LruCacheHit(method string) metricUtil.Percentage {
	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/client/lru/cache/hit/%v", method)
}

func (*ClientMetrics) DataCacheHit(method string) metricUtil.Percentage {
	return metricUtil.GetOrRegisterTimeWindowPercentageDefault(0, "infura/client/data/cache/hit/%v", method)
}
