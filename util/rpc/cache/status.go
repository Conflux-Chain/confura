package cache

import (
	"time"

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

func (c *StatusCache) GetStatus(nodeName string, cfx sdk.ClientOperator) (types.Status, bool, error) {
	val, loaded, err := c.inner.getOrUpdate(nodeName, func() (interface{}, error) {
		return cfx.GetStatus()
	})

	if err != nil {
		return types.Status{}, false, err
	}

	return val.(types.Status), loaded, nil
}

func (c *StatusCache) GetEpochNumber(nodeName string, cfx sdk.ClientOperator, epoch *types.Epoch) (*hexutil.Big, bool, error) {
	if types.EpochEarliest.Equals(epoch) {
		return types.NewBigInt(0), true, nil
	}

	status, loaded, err := c.GetStatus(nodeName, cfx)
	if err != nil {
		return nil, false, err
	}

	// latest mined by default
	if epoch == nil {
		return types.NewBigInt(uint64(status.EpochNumber)), loaded, nil
	}

	// epoch number
	if num, ok := epoch.ToInt(); ok {
		if num.Uint64() <= uint64(status.EpochNumber) {
			return types.NewBigIntByRaw(num), loaded, nil
		}

		epochNun, err := cfx.GetEpochNumber(epoch)
		return epochNun, false, err
	}

	// default epoch tags
	switch {
	case types.EpochLatestCheckpoint.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestCheckpoint)), loaded, nil
	case types.EpochLatestFinalized.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestFinalized)), loaded, nil
	case types.EpochLatestConfirmed.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestConfirmed)), loaded, nil
	case types.EpochLatestState.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestState)), loaded, nil
	}

	epochNun, err := cfx.GetEpochNumber(epoch)
	return epochNun, false, err
}

func (c *StatusCache) GetBestBlockHash(nodeName string, cfx sdk.ClientOperator) (types.Hash, bool, error) {
	status, loaded, err := c.GetStatus(nodeName, cfx)
	if err != nil {
		return "", false, err
	}

	return status.BestHash, loaded, nil
}
