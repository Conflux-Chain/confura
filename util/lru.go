package util

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
)

// expirableValue is used to hold value with expiration
type expirableValue struct {
	value     interface{}
	expiresAt time.Time
}

// TimeNowFunc returns current time
type TimeNowFunc func() time.Time

// ExpirableLruCache naive implementation of LRU cache with fixed TTL expiration duration.
// This cache uses a lazy eviction policy, by which the expired entry will be purged when
// it's being looked up.
type ExpirableLruCache struct {
	lru *lru.Cache
	mu  sync.Mutex
	ttl time.Duration

	// custom `time.Now` function, which could be used for testing
	timeNowFunc func() time.Time
}

func NewExpirableLruCache(size int, ttl time.Duration, timeNowFunc ...func() time.Time) *ExpirableLruCache {
	nowFunc := time.Now
	if len(timeNowFunc) > 0 {
		nowFunc = timeNowFunc[0]
	}
	cache, _ := lru.New(max(size, 1))
	return &ExpirableLruCache{lru: cache, ttl: ttl, timeNowFunc: nowFunc}
}

// Add adds a value to the cache. Returns true if an eviction occurred.
func (c *ExpirableLruCache) Add(key, value interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.add(key, value)
}

// Get looks up a key's value from the cache. Will purge the entry and return nil
// if the entry expired.
func (c *ExpirableLruCache) Get(key interface{}) (v interface{}, found bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	v, expired, found := c.get(key)
	if !found { // not found
		return nil, false
	}

	if expired { // expired
		c.lru.Remove(key)
		return nil, false
	}

	return v, true
}

// GetOrUpdate gets or updates the value for the given cache key.
// If the entry existed but expired, it will be purged and the updateFunc will be called.
// If the updateFunc returns an error, the function will return that error.
// If the entry existed and not expired, the function will return the existing value.
func (c *ExpirableLruCache) GetOrUpdate(key interface{}, updateFunc func() (interface{}, error)) (interface{}, error) {
	v, expired, found := c.get(key)
	if found && !expired {
		return v, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// double check for concurrency
	v, expired, found = c.get(key)
	if found && !expired {
		return v, nil
	}

	// update cache
	v, err := updateFunc()
	if err != nil {
		return nil, err
	}
	c.add(key, v)

	return v, nil
}

// GetWithoutExp looks up a key's value from the cache without expiration action.
func (c *ExpirableLruCache) GetWithoutExp(key interface{}) (v interface{}, expired, found bool) {
	return c.get(key)
}

func (c *ExpirableLruCache) get(key interface{}) (v interface{}, expired, found bool) {
	cv, ok := c.lru.Get(key) // not found
	if !ok {
		return nil, false, false
	}

	ev := cv.(*expirableValue)

	if ev.expiresAt.Before(c.timeNowFunc()) { // expired
		return ev.value, true, true
	}

	return ev.value, false, true
}

func (c *ExpirableLruCache) add(key, value interface{}) bool {
	ev := &expirableValue{
		value:     value,
		expiresAt: c.timeNowFunc().Add(c.ttl),
	}

	return c.lru.Add(key, ev)
}
