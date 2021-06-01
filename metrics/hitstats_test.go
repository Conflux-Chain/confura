package metrics

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHitStats(t *testing.T) {
	hs := NewHitStats(1, 1)
	assert.Equal(t, uint64(math.Pow(2, 32))+1, uint64(*hs))

	hits, visits := hs.Peek()
	assert.Equal(t, uint32(1), hits)
	assert.Equal(t, uint32(1), visits)

	hs = NewHitStats(2, 16)
	assert.Equal(t, uint64(math.Pow(2, 33))+16, uint64(*hs))

	hs.Incr(false)
	hits, visits = hs.Peek()
	assert.Equal(t, uint32(17), visits)
	assert.Equal(t, uint32(2), hits)

	hs.Incr(true)
	hits, visits = hs.Peek()
	assert.Equal(t, uint32(18), visits)
	assert.Equal(t, uint32(3), hits)

	ratio, ok := hs.GetRatio()
	assert.Equal(t, int64(3)*10000/18, ratio)
	assert.True(t, ok)
}
