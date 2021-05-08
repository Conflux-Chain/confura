package util

import (
	"sync"
)

type ConcurrentMap struct {
	sync.Map
	mu sync.Mutex
}

func (m *ConcurrentMap) LoadOrStoreFn(key interface{}, factory func(k interface{}) interface{}) (actual interface{}, loaded bool) {
	if val, ok := m.Load(key); ok {
		return val, true
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// double check
	if val, ok := m.Load(key); ok {
		return val, true
	}

	val := factory(key)
	m.Store(key, val)

	return val, false
}

func (m *ConcurrentMap) LoadOrStoreFnErr(key interface{}, factory func(k interface{}) (interface{}, error)) (actual interface{}, loaded bool, err error) {
	if val, ok := m.Load(key); ok {
		return val, true, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// double check
	if val, ok := m.Load(key); ok {
		return val, true, nil
	}

	val, err := factory(key)
	if err != nil {
		return nil, false, err
	}

	m.Store(key, val)

	return val, false, nil
}
