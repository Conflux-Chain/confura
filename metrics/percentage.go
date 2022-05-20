package metrics

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/metrics"
)

// Percentage implements the GaugeFloat64 interface for percentage statistic.
type Percentage interface {
	Mark(marked bool)
	Value() float64 // e.g. 99.38 means 99.38%
}

// NewPercentage constructs a new standard percentage metric.
func NewPercentage() Percentage {
	if !metrics.Enabled {
		return &noopPercentage{}
	}

	return &standardPercentage{}
}

// GetOrRegisterPercentage returns an existing Percentage or constructs and registers a new standard Percentage.
func GetOrRegisterPercentage(r metrics.Registry, name string, args ...interface{}) Percentage {
	return getOrRegisterPercentage(r, NewPercentage, name, args...)
}

// getOrRegisterPercentage gets or constructs Percentage with specified factory.
func getOrRegisterPercentage(r metrics.Registry, factory func() Percentage, name string, args ...interface{}) Percentage {
	if r == nil {
		r = metrics.DefaultRegistry
	}

	metricName := fmt.Sprintf(name, args...)

	return r.GetOrRegister(metricName, factory).(Percentage)
}

// noopPercentage is no-op implementation for Percentage interface.
type noopPercentage struct{}

func (p *noopPercentage) Mark(marked bool)               {}
func (p *noopPercentage) Value() float64                 { return 0 }
func (p *noopPercentage) Update(float64)                 {}
func (p *noopPercentage) Snapshot() metrics.GaugeFloat64 { return p }

type percentageData struct {
	total uint64
	marks uint64
}

func (data *percentageData) update(marked bool) {
	data.total++
	if marked {
		data.marks++
	}
}

// 10.19 means 10.19%
func (data *percentageData) value() float64 {
	if data.total == 0 {
		// percentage is 0 when never marked
		return 0
	}

	return float64(data.marks*10000/data.total) / 100
}

// standardPercentage is the standard implementation for Percentage interface.
type standardPercentage struct {
	data percentageData
	mu   sync.Mutex
}

func (p *standardPercentage) Mark(marked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.data.update(marked)
}

// Value implements the metrics.GaugeFloat64 interface.
func (p *standardPercentage) Value() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.data.value()
}

// Update implements the metrics.GaugeFloat64 interface.
func (p *standardPercentage) Update(float64) {
	panic("Update called on a standardPercentage")
}

// Snapshot implements the metrics.GaugeFloat64 interface.
func (p *standardPercentage) Snapshot() metrics.GaugeFloat64 {
	return metrics.GaugeFloat64Snapshot(p.Value())
}
