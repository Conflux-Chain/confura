package cache

import (
	"time"

	"github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// StatusCache memory cache for core space status related RPC method suites.
type StatusCache struct {
	inner *nodeExpiryCaches
}

func NewStatusCache() *StatusCache {
	return &StatusCache{
		// epoch increase every 1 second and different nodes have different epoch number
		inner: newNodeExpiryCaches(time.Second),
	}
}

func (c *StatusCache) GetStatus(cfx sdk.ClientOperator) (types.Status, error) {
	nodeName := rpc.Url2NodeName(cfx.GetNodeURL())

	val, err := c.inner.getOrUpdate(nodeName, func() (interface{}, error) {
		return cfx.GetStatus()
	})

	if err != nil {
		return types.Status{}, err
	}

	return val.(types.Status), nil
}

func (c *StatusCache) GetEpochNumber(cfx sdk.ClientOperator, epoch *types.Epoch) (*hexutil.Big, error) {
	if types.EpochEarliest.Equals(epoch) {
		return types.NewBigInt(0), nil
	}

	status, err := c.GetStatus(cfx)
	if err != nil {
		return nil, err
	}

	// latest mined by default
	if epoch == nil {
		return types.NewBigInt(uint64(status.EpochNumber)), nil
	}

	// epoch number
	if num, ok := epoch.ToInt(); ok {
		if num.Uint64() <= uint64(status.EpochNumber) {
			return types.NewBigIntByRaw(num), nil
		}

		return cfx.GetEpochNumber(epoch)
	}

	// default epoch tags
	switch {
	case types.EpochLatestCheckpoint.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestCheckpoint)), nil
	case types.EpochLatestFinalized.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestFinalized)), nil
	case types.EpochLatestConfirmed.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestConfirmed)), nil
	case types.EpochLatestState.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestState)), nil
	}

	return cfx.GetEpochNumber(epoch)
}

func (c *StatusCache) GetBestBlockHash(cfx sdk.ClientOperator) (types.Hash, error) {
	status, err := c.GetStatus(cfx)
	if err != nil {
		return "", err
	}

	return status.BestHash, nil
}
