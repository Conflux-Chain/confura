package metrics

import (
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

// twPercentageData time window percentage data
type twPercentageData percentageData

// implements `SlotData` interface

func (d twPercentageData) Add(v SlotData) SlotData {
	rhs := v.(twPercentageData)
	return twPercentageData{
		total: d.total + rhs.total,
		marks: d.marks + rhs.marks,
	}
}

func (d twPercentageData) Sub(v SlotData) SlotData {
	rhs := v.(twPercentageData)
	return twPercentageData{
		total: d.total - rhs.total,
		marks: d.marks - rhs.marks,
	}
}

func (d twPercentageData) SnapShot() SlotData {
	return d
}

// timeWindowPercentage implements Percentage interface to record recent percentage.
type timeWindowPercentage struct {
	window *TimeWindow
}

func newTimeWindowPercentage(slotInterval time.Duration, numSlots int) *timeWindowPercentage {
	return &timeWindowPercentage{
		window: NewTimeWindow(slotInterval, numSlots),
	}
}

func (p *timeWindowPercentage) Mark(marked bool) {
	data := twPercentageData{total: 1}
	if marked {
		data.marks++
	}

	p.window.Add(data)
}

// Value implements the metrics.GaugeFloat64 interface.
func (p *timeWindowPercentage) Value() float64 {
	aggdata := percentageData(p.window.Data().(twPercentageData))
	return aggdata.value()
}

// Update implements the metrics.GaugeFloat64 interface.
func (p *timeWindowPercentage) Update(float64) {
	panic("Update called on a timeWindowPercentage")
}

// Snapshot implements the metrics.GaugeFloat64 interface.
func (p *timeWindowPercentage) Snapshot() metrics.GaugeFloat64 {
	return metrics.GaugeFloat64Snapshot(p.Value())
}
