package metrics

import (
	"container/list"
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

// GetOrRegisterTimeWindowPercentage returns an existing Percentage or constructs and registers a new time window Percentage.
func GetOrRegisterTimeWindowPercentage(r metrics.Registry, slotInterval time.Duration, numSlots int, name string, args ...interface{}) Percentage {
	factory := func() Percentage {
		return NewTimeWindowPercentage(slotInterval, numSlots)
	}

	return getOrRegisterPercentage(r, factory, name, args...)
}

type slot struct {
	percentageData

	endTime  time.Time // end time to update percentage data
	expireAt time.Time // time to remove the slot
}

// timeWindowPercentage implements Percentage interface to record recent percentage.
type timeWindowPercentage struct {
	window percentageData

	slots          *list.List
	slotInterval   time.Duration
	windowInterval time.Duration

	mu sync.Mutex
}

func newTimeWindowPercentage(slotInterval time.Duration, numSlots int) *timeWindowPercentage {
	return &timeWindowPercentage{
		slots:          list.New(),
		slotInterval:   slotInterval,
		windowInterval: slotInterval * time.Duration(numSlots),
	}
}

// expire removes expired slots.
func (p *timeWindowPercentage) expire(now time.Time) {
	for {
		// time window is empty
		front := p.slots.Front()
		if front == nil {
			return
		}

		// not expired yet
		slot := front.Value.(*slot)
		if slot.expireAt.After(now) {
			return
		}

		// updates when slot expired
		p.slots.Remove(front)
		p.window.total -= slot.total
		p.window.marks -= slot.marks
	}
}

// addNewSlot always appends a new slot to time window.
func (p *timeWindowPercentage) addNewSlot(now time.Time) *slot {
	slotStartTime := now.Truncate(p.slotInterval)

	newSlot := &slot{
		endTime:  slotStartTime.Add(p.slotInterval),
		expireAt: slotStartTime.Add(p.windowInterval),
	}

	p.slots.PushBack(newSlot)

	return newSlot
}

// getOrAddSlot gets the last slot or adds a new slot if the last one out of date.
func (p *timeWindowPercentage) getOrAddSlot(now time.Time) *slot {
	// time window is empty
	if p.slots.Len() == 0 {
		return p.addNewSlot(now)
	}

	// last slot is not out of date
	lastSlot := p.slots.Back().Value.(*slot)
	if lastSlot.endTime.After(now) {
		return lastSlot
	}

	// otherwise, add new slot
	return p.addNewSlot(now)
}

func (p *timeWindowPercentage) Mark(marked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// remove expired slots
	now := time.Now()
	p.expire(now)

	// prepre slot to update
	currentSlot := p.getOrAddSlot(now)
	currentSlot.update(marked)

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
