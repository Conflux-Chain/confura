package cache

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExpiryCacheGet(t *testing.T) {
	cache := newExpiryCache(time.Minute)

	// no data by default
	val, ok := cache.get()
	assert.Nil(t, val)
	assert.False(t, ok)

	// add data into cache
	cache.getOrUpdate(func() (interface{}, error) {
		return "data", nil
	})

	// could get data
	val, ok = cache.get()
	assert.Equal(t, "data", val.(string))
	assert.True(t, ok)

	// timeout
	val, ok = cache.getAt(time.Now().Add(time.Minute + time.Nanosecond))
	assert.Nil(t, val)
	assert.False(t, ok)
}

func TestExpiryCacheGetOrUpdateWithError(t *testing.T) {
	cache := newExpiryCache(time.Minute)

	fooErr := errors.New("foo error")

	val, _, err := cache.getOrUpdate(func() (interface{}, error) {
		return "data", fooErr
	})

	assert.Nil(t, val)
	assert.Equal(t, fooErr, err)
}

func TestExpiryCacheGetOrUpdate(t *testing.T) {
	cache := newExpiryCache(time.Minute)

	// cache data
	val, cached, err := cache.getOrUpdate(func() (interface{}, error) {
		return "data", nil
	})
	assert.Equal(t, "data", val.(string))
	assert.Nil(t, err)
	assert.False(t, cached)

	// get cached data
	val, cached, err = cache.getOrUpdate(func() (interface{}, error) {
		return "data - 2", nil
	})
	assert.Equal(t, "data", val.(string))
	assert.Nil(t, err)
	assert.True(t, cached)

	// timeout and get new cached data
	val, cached, err = cache.getOrUpdateAt(time.Now().Add(time.Minute+time.Nanosecond), func() (interface{}, error) {
		return "data - 2", nil
	})
	assert.Equal(t, "data - 2", val.(string))
	assert.Nil(t, err)
	assert.False(t, cached)
}
