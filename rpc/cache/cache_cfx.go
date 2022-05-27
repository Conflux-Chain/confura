package cache

import (
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type CfxCache struct {
	*StatusCache

	priceCache   *expiryCache
	versionCache *expiryCache
}

func NewCfx() *CfxCache {
	return &CfxCache{
		StatusCache: NewStatusCache(),

		priceCache:   newExpiryCache(3 * time.Second),
		versionCache: newExpiryCache(time.Minute),
	}
}

func (cache *CfxCache) GetGasPrice(cfx sdk.ClientOperator) (*hexutil.Big, error) {
	val, err := cache.priceCache.getOrUpdate(func() (interface{}, error) {
		return cfx.GetGasPrice()
	})

	if err != nil {
		return nil, err
	}

	return val.(*hexutil.Big), nil
}

func (cache *CfxCache) GetClientVersion(cfx sdk.ClientOperator) (string, error) {
	val, err := cache.versionCache.getOrUpdate(func() (interface{}, error) {
		return cfx.GetClientVersion()
	})

	if err != nil {
		return "", err
	}

	return val.(string), nil
}
