package node

import (
	"context"
	"sync/atomic"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/buraksezer/consistent"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	_ Node = (*CfxNode)(nil)
	_ Node = (*EthNode)(nil)
)

// Node represents a full node with friendly name and status.
type Node interface {
	consistent.Member

	Name() string
	Url() string

	LatestEpochNumber() (uint64, error)

	Close()
}

type baseNode struct {
	name         string
	url          string
	cancel       context.CancelFunc
	atomicStatus atomic.Value
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

func (n *baseNode) monitor(ctx context.Context, node Node, hm HealthMonitor) {
	ticker := time.NewTicker(cfg.Monitor.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.WithField("name", n.name).Info("Complete to monitor node")
			return
		case <-ticker.C:
			status := n.atomicStatus.Load().(Status)
			status.Update(node, hm)
			n.atomicStatus.Store(status)
		}
	}
}

func (n *baseNode) Close() {
	n.cancel()
	status := n.Status()
	status.Close()
}

// EthNode represents an EVM space node with friendly name and status.
type EthNode struct {
	*web3go.Client
	*baseNode
}

// NewEthNode creates an instance of EVM space node and start to monitor
// node health in a separate goroutine until node closed.
func NewEthNode(name, url string, hm HealthMonitor) *EthNode {
	ctx, cancel := context.WithCancel(context.Background())
	requestTimeout := viper.GetDuration("eth.requestTimeout")

	n := &EthNode{
		baseNode: newBaseNode(name, url, cancel),
		Client:   util.MustNewEthClientWithRetry(url, 0, time.Millisecond, requestTimeout),
	}

	n.atomicStatus.Store(NewEthStatus(name))

	go n.monitor(ctx, n, hm)

	return n
}

func (n *EthNode) LatestEpochNumber() (uint64, error) {
	block, err := n.Eth.BlockNumber()
	if err != nil {
		return 0, err
	}

	if block == nil { // this should not happen, but anyway for robust
		return 0, errors.New("invalid block number")
	}

	return block.Uint64(), nil
}

// CfxNode represents a conflux node with friendly name and status.
type CfxNode struct {
	sdk.ClientOperator
	*baseNode
}

// NewCfxNode creates an instance of conflux full node and start to monitor
// node health in a separate goroutine until node closed.
func NewCfxNode(name, url string, hm HealthMonitor) *CfxNode {
	ctx, cancel := context.WithCancel(context.Background())
	requestTimeout := viper.GetDuration("cfx.requestTimeout")

	n := &CfxNode{
		baseNode:       newBaseNode(name, url, cancel),
		ClientOperator: util.MustNewCfxClientWithRetry(url, 0, time.Millisecond, requestTimeout),
	}

	n.atomicStatus.Store(NewStatus(name))

	go n.monitor(ctx, n, hm)

	return n
}

func (n *CfxNode) LatestEpochNumber() (uint64, error) {
	epoch, err := n.GetEpochNumber(types.EpochLatestMined)
	if err != nil {
		return 0, err
	}

	if epoch == nil { // this should not happen, but anyway for robust
		return 0, errors.New("invalid epoch number")
	}

	return epoch.ToInt().Uint64(), nil
}

func (n *CfxNode) Close() {
	n.baseNode.Close()
	n.ClientOperator.Close()
}
