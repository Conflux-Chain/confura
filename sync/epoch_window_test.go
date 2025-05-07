package sync

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEpochWindow(t *testing.T) {
	ew := newEpochWindow(1000) // create an uninitialized epoch window
	assert.Equal(t, uint32(0), ew.size())
	assert.True(t, ew.isEmpty())
	assert.False(t, ew.isSet())

	ew.reset(15023, 15923)
	assert.Equal(t, uint32(1+(15923-15023)), ew.size())
	assert.False(t, ew.isEmpty())
	assert.True(t, ew.isSet())

	ew.reset(19252, 19102)
	assert.Equal(t, uint32(0), ew.size())
	assert.True(t, ew.isEmpty())
	assert.True(t, ew.isSet())
}

func TestEpochWindowExpandingFrom(t *testing.T) {
	ew := newEpochWindow(1000)

	ew.expandFrom(14575) // expand uninitialized epoch window
	assert.Equal(t, uint32(1), ew.size())
	assert.False(t, ew.isEmpty())
	assert.True(t, ew.isSet())

	ew.expandFrom(14580) // expand epoch window from some invalid upper bound
	assert.Equal(t, uint32(1), ew.size())

	ew.expandFrom(14570) // expand epoch window from some valid lower bound
	assert.Equal(t, uint32(1+(14575-14570)), ew.size())
}

func TestEpochWindowExpandingTo(t *testing.T) {
	ew := newEpochWindow(1000)

	ew.expandTo(14570) // expand uninitialized epoch window
	assert.Equal(t, uint32(1), ew.size())
	assert.False(t, ew.isEmpty())
	assert.True(t, ew.isSet())

	ew.expandTo(14565) // expand epoch window from some invalid lower bound
	assert.Equal(t, uint32(1), ew.size())

	ew.expandTo(14579) // expand epoch window to some valid upper bound
	assert.Equal(t, uint32(1+(14579-14570)), ew.size())
}

func TestEpochWindowExpanding(t *testing.T) {
	ew := newEpochWindow(1000)

	ew.expandFrom(14935) // expand uninitialized epoch window
	assert.Equal(t, uint32(1), ew.size())
	assert.False(t, ew.isEmpty())
	assert.True(t, ew.isSet())

	ew.expandTo(14970) // expand epoch window to some valid upper bound
	assert.Equal(t, uint32(1+(14970-14935)), ew.size())
}

func TestEpochWindowPeekWillPivotSwitch(t *testing.T) {
	ew := newEpochWindow(1000)
	assert.False(t, ew.peekWillPivotSwitch(0))

	ew.reset(15923, 15793)
	assert.False(t, ew.peekWillPivotSwitch(15983))
	assert.False(t, ew.peekWillPivotSwitch(15923))
	assert.True(t, ew.peekWillPivotSwitch(15922))
}

func TestEpochWindowPeekWillOverflow(t *testing.T) {
	ew := newEpochWindow(1000)
	assert.False(t, ew.peekWillOverflow(13202))

	ew.reset(21723, 21883)
	assert.False(t, ew.peekWillOverflow(21953))
	assert.True(t, ew.peekWillOverflow(23293))
}

func TestEpochWindowShrinkFrom(t *testing.T) {
	ew := newEpochWindow(1000)
	sf, ss := ew.peekShrinkFrom(10)
	assert.Equal(t, uint64(0), sf)
	assert.Equal(t, uint32(0), ss)

	ew.reset(23892, 24503)
	sf, ss = ew.peekShrinkFrom(5000)
	assert.Equal(t, ew.epochFrom, sf)
	assert.Equal(t, min(ew.size(), 5000), ss)

	sf2, ss2 := ew.shrinkFrom(5000)
	assert.Equal(t, sf, sf2)
	assert.Equal(t, ss, ss2)
	assert.True(t, ew.isEmpty())
}
