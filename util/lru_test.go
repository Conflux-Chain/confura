package util_test

import (
	"sync"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/util"
	"github.com/pkg/errors"
)

// mockTime is used to simulate time progression in tests.
type mockTime struct {
	mu          sync.Mutex
	currentTime time.Time
}

func (m *mockTime) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentTime
}

func (m *mockTime) Add(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTime = m.currentTime.Add(d)
}

// TestAddAndGet tests the basic Add and Get functionality.
func TestAddAndGet(t *testing.T) {
	cache := util.NewExpirableLruCache(5, 50*time.Millisecond)
	cache.Add("key1", "value1")

	value, found := cache.Get("key1")
	if !found || value != "value1" {
		t.Errorf("Expected to find key1 with value 'value1', got '%v', found: %v", value, found)
	}
}

// TestExpiration tests that entries expire after the TTL.
func TestExpiration(t *testing.T) {
	mt := &mockTime{currentTime: time.Now()}
	cache := util.NewExpirableLruCache(5, 50*time.Millisecond, mt.Now)
	cache.Add("key1", "value1")

	// Simulate time passing beyond the TTL
	mt.Add(60 * time.Millisecond)

	value, found := cache.Get("key1")
	if found || value != nil {
		t.Errorf("Expected key1 to be expired and not found, got '%v', found: %v", value, found)
	}
}

// TestGetOrUpdate tests the GetOrUpdate method.
func TestGetOrUpdate(t *testing.T) {
	mt := &mockTime{currentTime: time.Now()}
	cache := util.NewExpirableLruCache(5, 50*time.Millisecond, mt.Now)

	updateFunc := func() (interface{}, error) {
		return "value1", nil
	}

	// Initially, the key does not exist, so updateFunc should be called
	value, err := cache.GetOrUpdate("key1", updateFunc)
	if err != nil || value != "value1" {
		t.Errorf("Expected to get 'value1', got '%v', error: %v", value, err)
	}

	// Fetch again; since it's not expired, should return cached value
	value, err = cache.GetOrUpdate("key1", func() (interface{}, error) {
		return "value2", nil
	})
	if err != nil || value != "value1" {
		t.Errorf("Expected to get 'value1' from cache, got '%v', error: %v", value, err)
	}

	// Advance time to expire the entry
	mt.Add(60 * time.Millisecond)

	// Now, updateFunc should be called again
	value, err = cache.GetOrUpdate("key1", func() (interface{}, error) {
		return "value3", nil
	})
	if err != nil || value != "value3" {
		t.Errorf("Expected to get 'value3' after expiration, got '%v', error: %v", value, err)
	}
}

// TestGetWithoutExp tests the GetWithoutExp method.
func TestGetWithoutExp(t *testing.T) {
	mt := &mockTime{currentTime: time.Now()}
	cache := util.NewExpirableLruCache(5, 50*time.Millisecond, mt.Now)
	cache.Add("key1", "value1")

	value, expired, found := cache.GetWithoutExp("key1")
	if !found || expired {
		t.Errorf("Expected to find key1 not expired, found: %v, expired: %v", found, expired)
	}
	if value != "value1" {
		t.Errorf("Expected value 'value1', got '%v'", value)
	}

	// Advance time to expire the entry
	mt.Add(60 * time.Millisecond)

	value, expired, found = cache.GetWithoutExp("key1")
	if !found || !expired {
		t.Errorf("Expected to find key1 expired, found: %v, expired: %v", found, expired)
	}
	if value != "value1" {
		t.Errorf("Expected value 'value1', got '%v'", value)
	}
}

// TestLRUEviction tests the LRU eviction policy of the cache.
func TestLRUEviction(t *testing.T) {
	mt := &mockTime{currentTime: time.Now()}
	cache := util.NewExpirableLruCache(3, time.Minute, mt.Now)
	cache.Add("key1", "value1")
	cache.Add("key2", "value2")
	cache.Add("key3", "value3")

	// Access key1 and key2 to make key3 the least recently used
	cache.Get("key1")
	cache.Get("key2")

	// Add a new key to trigger eviction
	cache.Add("key4", "value4")

	// key3 should have been evicted
	_, found := cache.Get("key3")
	if found {
		t.Errorf("Expected key3 to be evicted, but it was found")
	}

	// key1, key2, key4 should still be present
	for _, k := range []string{"key1", "key2", "key4"} {
		if _, found := cache.Get(k); !found {
			t.Errorf("Expected %s to be present, but it was not found", k)
		}
	}
}

// TestUpdateFuncError tests the behavior when updateFunc returns an error.
func TestUpdateFuncError(t *testing.T) {
	mt := &mockTime{currentTime: time.Now()}
	cache := util.NewExpirableLruCache(5, 50*time.Millisecond, mt.Now)
	updateFunc := func() (interface{}, error) {
		return nil, errors.New("update failed")
	}

	value, err := cache.GetOrUpdate("key1", updateFunc)
	if err == nil || value != nil {
		t.Errorf("Expected error 'update failed', got value '%v', error: %v", value, err)
	}
}
