package rpc

import (
	"encoding/json"
	"sync/atomic"
)

// LazyJSON is a struct that holds a lazily-loaded JSON object. It reduces redundant
// JSON marshalling and unmarshalling to improve performance by caching the JSON byte slice.
type LazyJSON[T any] struct {
	// cachedBytes holds the cached JSON byte slice after the first marshalling.
	cachedBytes json.RawMessage
	// value stores the original object, using atomic for thread-safety.
	value atomic.Value
}

// NewLazyJSON creates a new LazyJSON instance. It accepts an initial value that will be stored lazily.
func NewLazyJSON[T any](initialValue T) *LazyJSON[T] {
	lazyJSON := &LazyJSON[T]{}
	lazyJSON.value.Store(initialValue)
	return lazyJSON
}

// MarshalJSON implements the json.Marshaler interface.
// It marshals the object only once and then caches the result for future use.
func (l *LazyJSON[T]) MarshalJSON() ([]byte, error) {
	// If cachedBytes is already set, return it directly
	if l.cachedBytes != nil {
		return l.cachedBytes, nil
	}
	// Otherwise, marshal the original value
	rawData, err := json.Marshal(l.value.Load().(T))
	if err != nil {
		return nil, err
	}
	// Cache the marshalled JSON bytes
	l.cachedBytes = rawData
	return rawData, nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
// It unmarshals the JSON data and stores it as the original value.
func (l *LazyJSON[T]) UnmarshalJSON(data []byte) error {
	var decodedValue T
	if v := l.value.Load(); v != nil {
		decodedValue = v.(T)
	}
	if err := json.Unmarshal(data, &decodedValue); err != nil {
		return err
	}
	// Store the decoded value atomically
	l.value.Store(decodedValue)
	// Cache the raw JSON bytes
	l.cachedBytes = data
	return nil
}

// GetOriginal returns the original object stored in the LazyJSON.
func (l *LazyJSON[T]) GetOriginal() (T, bool) {
	v, ok := l.value.Load().(T)
	return v, ok
}
