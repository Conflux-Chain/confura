package cache

import (
	"fmt"
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// StatusCache memory cache for core space status related RPC method suites.
type StatusCache struct {
	inner         *nodeExpiryCaches
	epochCache    *keyExpiryLruCaches
	bestHashCache *nodeExpiryCaches
}

func NewStatusCache() *StatusCache {
	return &StatusCache{
		// epoch increase every 1 second and different nodes have different epoch number
		inner:         newNodeExpiryCaches(time.Second),
		bestHashCache: newNodeExpiryCaches(time.Second),
		epochCache:    newKeyExpiryLruCaches(time.Second, 1000),
	}
}

func (c *StatusCache) GetStatus(nodeName string, cfx sdk.ClientOperator) (types.Status, bool, error) {
	return c.GetStatusWithFunc(nodeName, func() (interface{}, error) {
		return cfx.GetStatus()
	})
}

func (c *StatusCache) GetStatusWithFunc(nodeName string, rawGetter func() (interface{}, error)) (types.Status, bool, error) {
	val, loaded, err := c.inner.getOrUpdate(nodeName, func() (interface{}, error) {
		return rawGetter()
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

	epochNum := c.getEpochNumberFromStatus(status, epoch)
	if epochNum != nil {
		return epochNum, loaded, nil
	}

	epochNun, err := cfx.GetEpochNumber(epoch)
	return epochNun, false, err
}

func (c *StatusCache) GetEpochNumberWithFunc(
	nodeName string, rawGetter func() (interface{}, error), epoch *types.Epoch) (*hexutil.Big, bool, error) {
	if types.EpochEarliest.Equals(epoch) {
		return types.NewBigInt(0), true, nil
	}

	// load from status cache at first
	if val, ok := c.inner.node2Caches.Load(nodeName); ok {
		if status, ok := val.(types.Status); ok {
			epochNum := c.getEpochNumberFromStatus(status, epoch)
			if epochNum != nil {
				return epochNum, true, nil
			}
		}
	}

	// latest mined by default
	if epoch == nil {
		epoch = types.EpochLatestMined
	}

	// otherwises load from epoch cache
	cacheKey := fmt.Sprintf("%s_%s", nodeName, epoch.String())
	val, loaded, err := c.epochCache.getOrUpdate(cacheKey, func() (interface{}, error) {
		return rawGetter()
	})
	if err != nil {
		return nil, false, err
	}
	return val.(*hexutil.Big), loaded, nil
}

func (c *StatusCache) GetBestBlockHash(nodeName string, cfx sdk.ClientOperator) (types.Hash, bool, error) {
	status, loaded, err := c.GetStatus(nodeName, cfx)
	if err != nil {
		return "", false, err
	}
	return status.BestHash, loaded, nil
}

func (c *StatusCache) GetBestBlockHashWithFunc(nodeName string, rawGetter func() (interface{}, error)) (types.Hash, bool, error) {
	// load from status cache at first
	if val, ok := c.inner.node2Caches.Load(nodeName); ok {
		if status, ok := val.(types.Status); ok {
			return status.BestHash, true, nil
		}
	}

	// otherwise load from best hash cache
	val, loaded, err := c.bestHashCache.getOrUpdate(nodeName, func() (interface{}, error) {
		return rawGetter()
	})
	if err != nil {
		return "", false, err
	}
	return val.(types.Hash), loaded, nil
}

func (c *StatusCache) getEpochNumberFromStatus(status types.Status, epoch *types.Epoch) *hexutil.Big {
	// latest mined by default
	if epoch == nil {
		return types.NewBigInt(uint64(status.EpochNumber))
	}

	// epoch number
	if num, ok := epoch.ToInt(); ok {
		if num.Uint64() <= uint64(status.EpochNumber) {
			return types.NewBigIntByRaw(num)
		}
		return nil
	}

	// default epoch tags
	switch {
	case types.EpochLatestCheckpoint.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestCheckpoint))
	case types.EpochLatestFinalized.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestFinalized))
	case types.EpochLatestConfirmed.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestConfirmed))
	case types.EpochLatestState.Equals(epoch):
		return types.NewBigInt(uint64(status.LatestState))
	}

	// unrecognized
	return nil
}
