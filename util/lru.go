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

// ExpirableLruCache naive implementation of LRU cache with fixed TTL expiration duration.
// This cache uses a lazy eviction policy, by which the expired entry will be purged when
// it's being looked up.
type ExpirableLruCache struct {
	lru *lru.Cache
	mu  sync.Mutex
	ttl time.Duration
}

func NewExpirableLruCache(size int, ttl time.Duration) *ExpirableLruCache {
	cache, _ := lru.New(size)
	return &ExpirableLruCache{lru: cache, ttl: ttl}
}

// Add adds a value to the cache. Returns true if an eviction occurred.
func (c *ExpirableLruCache) Add(key, value interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ev := &expirableValue{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}

	return c.lru.Add(key, ev)
}

// Get looks up a key's value from the cache. Will purge the entry and return nil
// if the entry expired.
func (c *ExpirableLruCache) Get(key interface{}) (v interface{}, found bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cv, ok := c.lru.Get(key) // not found
	if !ok {
		return nil, false
	}

	ev := cv.(*expirableValue)
	if ev.expiresAt.Before(time.Now()) { // expired
		c.lru.Remove(key)
		return ev.value, false
	}

	return ev.value, true
}

// Get looks up a key's value from the cache without expiration action.
func (c *ExpirableLruCache) GetNoExp(key interface{}) (v interface{}, expired, found bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cv, ok := c.lru.Get(key) // not found
	if !ok {
		return nil, false, false
	}

	ev := cv.(*expirableValue)

	if ev.expiresAt.Before(time.Now()) { // expired
		return ev.value, true, true
	}

	return ev.value, false, true
}
