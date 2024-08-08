package node

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/buraksezer/consistent"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	_ Node = (*CfxNode)(nil)
	_ Node = (*EthNode)(nil)
)

// Node represents a full node with friendly name and health status.
type Node interface {
	consistent.Member

	Name() string
	Url() string
	Status() Status
	LatestEpochNumber() (uint64, error)

	Register(HealthMonitor)
	Deregister(HealthMonitor)

	Close()
}

type baseNode struct {
	mu           sync.Mutex
	name         string
	url          string
	cancel       context.CancelFunc
	atomicStatus atomic.Value
	monitors     []HealthMonitor
}

func newBaseNode(name, url string, cancel context.CancelFunc) *baseNode {
	return &baseNode{
		name: name, url: url, cancel: cancel,
	}
}

func (n *baseNode) Name() string {
	return n.name
}

func (n *baseNode) Url() string {
	return n.url
}

func (n *baseNode) Status() Status {
	return n.atomicStatus.Load().(Status)
}

func (n *baseNode) String() string {
	return n.name
}

// monitor periodically heartbeats with node to monitor health status
func (n *baseNode) monitor(ctx context.Context, node Node) {
	ticker := time.NewTicker(cfg.Monitor.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.WithField("name", n.name).Info("Complete to monitor node")
			return
		case <-ticker.C:
			status := n.atomicStatus.Load().(Status)
			status.Update(node)

			monitors := n.snapshot()
			for _, hm := range monitors {
				status.Report(hm)
			}

			logrus.WithFields(logrus.Fields{
				"numHealthMonitors": len(monitors),
				"status":            status,
			}).Debug("Node health status reported")

			n.atomicStatus.Store(status)
		}
	}
}

// snapshot snapshots all health monitors.
func (n *baseNode) snapshot() []HealthMonitor {
	n.mu.Lock()
	defer n.mu.Unlock()

	res := make([]HealthMonitor, 0, len(n.monitors))
	return append(res, n.monitors...)
}

// Register registers a health monitor to node.
func (n *baseNode) Register(hm HealthMonitor) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.monitors = append(n.monitors, hm)
}

// Deregister deregisters a health monitor from node.
func (n *baseNode) Deregister(hm HealthMonitor) {
	n.mu.Lock()
	defer n.mu.Unlock()

	monitors := []HealthMonitor{}
	for _, m := range n.monitors {
		if m != hm {
			monitors = append(monitors, m)
		}
	}
	n.monitors = monitors
}

func (n *baseNode) Close() {
	n.cancel()

	status := n.Status()
	status.Close()
}

// EthNode represents an evm space node with friendly name and health status.
type EthNode struct {
	*web3go.Client
	*baseNode
}

// NewEthNode creates an instance of evm space node and start to monitor
// node health in a separate goroutine until node closed.
func NewEthNode(group Group, name, url string) (*EthNode, error) {
	eth, err := rpc.NewEthClient(url)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &EthNode{
		baseNode: newBaseNode(name, url, cancel),
		Client:   eth,
	}

	n.atomicStatus.Store(NewStatus(group, name))

	go n.monitor(ctx, n)

	return n, nil
}

// LatestEpochNumber returns the latest block height of the evm space fullnode
func (n *EthNode) LatestEpochNumber() (uint64, error) {
	block, err := n.Eth.BlockNumber()
	if err != nil {
		return 0, err
	}

	if block == nil { // this shouldn't happen, but just in case...
		logrus.WithField("node", n).Info("Failed to get latest block number (nil) from eth node")
		return 0, errors.New("invalid block number")
	}

	return block.Uint64(), nil
}

// CfxNode represents a core space fullnode with friendly name and health status.
type CfxNode struct {
	sdk.ClientOperator
	*baseNode
}

// NewCfxNode creates an instance of core space fullnode and start to monitor
// node health in a separate goroutine until node closed.
func NewCfxNode(group Group, name, url string) (*CfxNode, error) {
	cfx, err := rpc.NewCfxClient(url)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &CfxNode{
		baseNode:       newBaseNode(name, url, cancel),
		ClientOperator: cfx,
	}

	n.atomicStatus.Store(NewStatus(group, name))

	go n.monitor(ctx, n)

	return n, nil
}

// LatestEpochNumber returns the latest epoch height of the core space fullnode
func (n *CfxNode) LatestEpochNumber() (uint64, error) {
	epoch, err := n.GetEpochNumber(types.EpochLatestMined)
	if err != nil {
		return 0, err
	}

	if epoch == nil { // this should not happen, but just in case...
		logrus.WithField("node", n).Info("Failed to get latest epoch number (nil) from node")
		return 0, errors.New("invalid epoch number")
	}

	return epoch.ToInt().Uint64(), nil
}

func (n *CfxNode) Close() {
	n.baseNode.Close()
	n.ClientOperator.Close()
}
