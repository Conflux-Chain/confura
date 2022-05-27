package cache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/conflux-chain/conflux-infura/util"
)

// for atomic load/store in cache.
type cacheValue struct {
	value    interface{}
	expireAt time.Time
}

// expiryCache is used to cache value with specified expiration time.
type expiryCache struct {
	value   atomic.Value
	timeout time.Duration
	mu      sync.Mutex
}

func newExpiryCache(timeout time.Duration) *expiryCache {
	return &expiryCache{
		timeout: timeout,
	}
}

func (cache *expiryCache) get() (interface{}, bool) {
	return cache.getAt(time.Now())
}

func (cache *expiryCache) getAt(time time.Time) (interface{}, bool) {
	value := cache.value.Load()
	if value == nil {
		return nil, false
	}

	val := value.(cacheValue)
	if val.expireAt.Before(time) {
		return nil, false
	}

	return val.value, true
}

func (cache *expiryCache) getOrUpdate(updateFunc func() (interface{}, error)) (interface{}, error) {
	return cache.getOrUpdateAt(time.Now(), updateFunc)
}

func (cache *expiryCache) getOrUpdateAt(time time.Time, updateFunc func() (interface{}, error)) (interface{}, error) {
	// cache value not expired
	if val, ok := cache.getAt(time); ok {
		return val, nil
	}

	// otherwise, query from fullnode and cache
	cache.mu.Lock()
	defer cache.mu.Unlock()

	// double check for concurrency
	if val, ok := cache.getAt(time); ok {
		return val, nil
	}

	val, err := updateFunc()
	if err != nil {
		return nil, err
	}

	// update cache
	cache.value.Store(cacheValue{
		value:    val,
		expireAt: time.Add(cache.timeout),
	})

	return val, nil
}

// nodeExpiryCaches is used for multiple nodes to cache data respectively.
type nodeExpiryCaches struct {
	node2Caches util.ConcurrentMap // node name => expiryCache
	timeout     time.Duration
}

func newNodeExpiryCaches(timeout time.Duration) *nodeExpiryCaches {
	return &nodeExpiryCaches{
		timeout: timeout,
	}
}

func (caches *nodeExpiryCaches) getOrUpdate(node string, updateFunc func() (interface{}, error)) (interface{}, error) {
	val, _ := caches.node2Caches.LoadOrStoreFn(node, func(interface{}) interface{} {
		return newExpiryCache(caches.timeout)
	})

	return val.(*expiryCache).getOrUpdate(updateFunc)
}
