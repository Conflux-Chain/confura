package cache

import (
	"time"

	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var CfxDefault = NewCfx()

// CfxCache memory cache for some core space RPC methods
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

func (cache *CfxCache) GetGasPrice(cfx sdk.ClientOperator) (*hexutil.Big, bool, error) {
	return cache.GetGasPriceWithFunc(cfx.GetGasPrice)
}

func (cache *CfxCache) GetGasPriceWithFunc(rawGetter func() (*hexutil.Big, error)) (*hexutil.Big, bool, error) {
	val, loaded, err := cache.priceCache.getOrUpdate(func() (any, error) {
		return rawGetter()
	})
	if err != nil {
		return nil, false, err
	}
	return val.(*hexutil.Big), loaded, nil
}

func (cache *CfxCache) GetClientVersion(cfx sdk.ClientOperator) (string, bool, error) {
	return cache.GetClientVersionWithFunc(cfx.GetClientVersion)
}

func (cache *CfxCache) GetClientVersionWithFunc(rawGetter func() (string, error)) (string, bool, error) {
	val, loaded, err := cache.versionCache.getOrUpdate(func() (any, error) {
		return rawGetter()
	})
	if err != nil {
		return "", false, err
	}
	return val.(string), loaded, nil
}
