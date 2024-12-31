package rpc

import (
	"encoding/json"
	"sync/atomic"
)

// lazyDecodedJsonObject is a generic structure that supports lazy decoding of JSON objects.
// This approach avoids unnecessary JSON unmarshalling until the value is explicitly accessed,
// which can improve performance when the decoded value is not always needed.
type lazyDecodedJsonObject[T any] struct {
	cachedBytes json.RawMessage // Holds the raw JSON bytes for deferred decoding.
	value       atomic.Value    // Caches the decoded value for thread-safe access.
}

// MarshalJSON implements the json.Marshaler interface.
// It returns the cached JSON bytes, ensuring the object can be marshaled back to JSON
// without decoding the actual value.
func (obj *lazyDecodedJsonObject[T]) MarshalJSON() ([]byte, error) {
	return obj.cachedBytes, nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
// It stores the raw JSON bytes for lazy decoding, which allows the object to defer expensive
// JSON unmarshalling until the value is explicitly accessed.
func (obj *lazyDecodedJsonObject[T]) UnmarshalJSON(b []byte) error {
	obj.cachedBytes = b
	return nil
}

// Load returns the lazily decoded value. If the value has not been decoded yet,
// it unmarshals the cached JSON bytes, caches the result, and returns it. Subsequent
// calls will return the cached value.
func (obj *lazyDecodedJsonObject[T]) Load() (T, error) {
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
