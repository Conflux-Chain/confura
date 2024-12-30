package rpc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLazyJSONMarshalJSON tests the MarshalJSON method of LazyJSON.
func TestLazyJSONMarshalJSON(t *testing.T) {
	// Create an initial object with some data
	obj := NewLazyJSON(map[string]string{"name": "John", "age": "30"})

	// First call to MarshalJSON should serialize the object and cache the result
	data, err := obj.MarshalJSON()
	assert.NoError(t, err) // Check for no error

	// Ensure the serialized data is valid JSON
	var result map[string]string
	err = json.Unmarshal(data, &result)
	assert.NoError(t, err) // Check for valid JSON

	// Ensure cachedBytes is set after the first call
	assert.NotNil(t, obj.cachedBytes, "expected cachedBytes to be set after first MarshalJSON call")

	// Second call to MarshalJSON should return cached data
	data2, err := obj.MarshalJSON()
	assert.NoError(t, err) // Check for no error

	// Ensure the second call returns the cached data (no re-serialization)
	assert.Equal(t, data, data2, "expected cached data to be reused")
}

// TestLazyJSONUnmarshalJSON tests the UnmarshalJSON method of LazyJSON.
func TestLazyJSONUnmarshalJSON(t *testing.T) {
	// Initialize JSON data
	data := []byte(`{"name": "Alice", "age": "25"}`)

	// Create a new LazyJSON object with an empty initial value
	obj := NewLazyJSON(map[string]string{})

	// Call UnmarshalJSON to deserialize the data
	err := obj.UnmarshalJSON(data)
	assert.NoError(t, err) // Check for no error

	// Ensure the object is correctly unmarshalled
	original, ok := obj.GetOriginal()
	assert.True(t, ok, "expected original object to be set")
	assert.Equal(t, "Alice", original["name"], "expected 'name' to be 'Alice'")
	assert.Equal(t, "25", original["age"], "expected 'age' to be '25'")

	// Ensure the cachedBytes hold the same raw JSON data
	assert.Equal(t, string(data), string(obj.cachedBytes), "expected cachedBytes to be the same as the input data")
}
