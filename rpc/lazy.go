package rpc

import (
	"encoding/json"
	"sync/atomic"
)

type lazyJsonObject[T any] struct {
	cachedBytes json.RawMessage
	value       atomic.Value
}

func (obj *lazyJsonObject[T]) MarshalJSON() ([]byte, error) {
	return obj.cachedBytes, nil
}

func (obj *lazyJsonObject[T]) UnmarshalJSON(b []byte) error {
	obj.cachedBytes = b
	return nil
}

func (obj *lazyJsonObject[T]) Load() (T, error) {
	if v, ok := obj.value.Load().(T); ok {
		return v, nil
	}
	var val T
	if err := json.Unmarshal(obj.cachedBytes, &val); err != nil {
		return val, err
	}
	obj.value.Store(val)
	return val, nil
}
