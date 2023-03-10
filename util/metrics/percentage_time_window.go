package metrics

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

// NewTimeWindowPercentage constructs a new time window Percentage.
func NewTimeWindowPercentage(slotInterval time.Duration, numSlots int) Percentage {
	if slotInterval == 0 {
		panic("slotInterval is zero")
	}

	if numSlots <= 1 {
		panic("numSlots too small")
	}

	if !metrics.Enabled {
		return &noopPercentage{}
	}

	return newTimeWindowPercentage(slotInterval, numSlots)
}

// GetOrRegisterTimeWindowPercentageDefault returns an existing Percentage or constructs and
// registers a new time window Percentage.
func GetOrRegisterTimeWindowPercentageDefault(name string, args ...interface{}) Percentage {
	factory := func() Percentage {
		return NewTimeWindowPercentage(time.Minute, 10)
	}

	return getOrRegisterPercentage(factory, name, args...)
}

// GetOrRegisterTimeWindowPercentage returns an existing Percentage or constructs and registers
// a new time window Percentage.
func GetOrRegisterTimeWindowPercentage(
	slotInterval time.Duration, numSlots int, name string, args ...interface{},
) Percentage {
	factory := func() Percentage {
		return NewTimeWindowPercentage(slotInterval, numSlots)
	}

	return getOrRegisterPercentage(factory, name, args...)
}

// percentageSlot time window slot for percentage
type percentageSlot struct {
	slotContext

	data percentageData
}

func newPercentageSlot(ctx slotContext) slot {
	return &percentageSlot{slotContext: ctx}
}

// implement `slot` interface
func (ps *percentageSlot) update(v interface{}) {
	if marked, ok := v.(bool); ok {
		ps.data.update(marked)
	}
}

func (ps *percentageSlot) snapshot() interface{} {
	return ps.data
}

// timeWindowPercentage implements Percentage interface to record recent percentage.
type timeWindowPercentage struct {
	*slotter

	mu     sync.Mutex
	window percentageData
}

func newTimeWindowPercentage(slotInterval time.Duration, numSlots int) *timeWindowPercentage {
	return &timeWindowPercentage{
		slotter: newSlotter(slotInterval, numSlots),
	}
}

func (p *timeWindowPercentage) Mark(marked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// remove expired slots
	now := time.Now()
	eslots := p.expire(now)

	// update window changes brought by expired slots
	for i := range eslots {
		data := eslots[i].snapshot().(percentageData)
		p.window.total -= data.total
		p.window.marks -= data.marks
	}

	// prepare slot to update
	curslot := p.getOrAddSlot(now, newPercentageSlot)
	curslot.update(marked)

	p.window.update(marked)
}

// Value implements the metrics.GaugeFloat64 interface.
func (p *timeWindowPercentage) Value() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	// remove expired slots
	now := time.Now()
	p.expire(now)

	return p.window.value()
}

// Update implements the metrics.GaugeFloat64 interface.
func (p *timeWindowPercentage) Update(float64) {
	panic("Update called on a timeWindowPercentage")
}

// Snapshot implements the metrics.GaugeFloat64 interface.
func (p *timeWindowPercentage) Snapshot() metrics.GaugeFloat64 {
	return metrics.GaugeFloat64Snapshot(p.Value())
}
