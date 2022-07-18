package service

import (
	"sort"
	"time"

	"github.com/Conflux-Chain/confura/util/metrics"
)

const Namespace = "metrics"

type MetricsAPI struct{}

// Query APIs
func (api *MetricsAPI) List() []string {
	var names []string

	for k := range metrics.InfuraRegistry.GetAll() {
		names = append(names, k)
	}

	sort.Strings(names)

	return names
}

func (api *MetricsAPI) Get(name string) map[string]interface{} {
	return metrics.InfuraRegistry.GetAll()[name]
}

func (api *MetricsAPI) All() map[string]map[string]interface{} {
	names := api.List()

	content := make(map[string]map[string]interface{})

	for _, v := range names {
		content[v] = metrics.InfuraRegistry.GetAll()[v]
	}

	return content
}

// Counter
func (api *MetricsAPI) ClearCounter(name string) {
	metrics.GetOrRegisterCounter(name).Clear()
}

func (api *MetricsAPI) DecCounter(name string, i int64) {
	metrics.GetOrRegisterCounter(name).Dec(i)
}

func (api *MetricsAPI) IncCounter(name string, i int64) {
	metrics.GetOrRegisterCounter(name).Inc(i)
}

// Gauge
func (api *MetricsAPI) UpdateGauge(name string, v int64) {
	metrics.GetOrRegisterGauge(name).Update(v)
}

func (api *MetricsAPI) DecGauge(name string, i int64) {
	metrics.GetOrRegisterGauge(name).Dec(i)
}

func (api *MetricsAPI) IncGauge(name string, i int64) {
	metrics.GetOrRegisterGauge(name).Inc(i)
}

// GaugeFloat64
func (api *MetricsAPI) UpdateGaugeFloat64(name string, v float64) {
	metrics.GetOrRegisterGaugeFloat64(name).Update(v)
}

// Meter
func (api *MetricsAPI) MarkMeter(name string, n int64) {
	metrics.GetOrRegisterMeter(name).Mark(n)
}

func (api *MetricsAPI) StopMeter(name string) {
	metrics.GetOrRegisterMeter(name).Stop()
}

// Histogram
func (api *MetricsAPI) ClearHistogram(name string) {
	metrics.GetOrRegisterHistogram(name).Clear()
}

func (api *MetricsAPI) UpdateHistogram(name string, v int64) {
	metrics.GetOrRegisterHistogram(name).Update(v)
}

// Timer
func (api *MetricsAPI) UpdateTimer(name string, v int64) {
	metrics.GetOrRegisterTimer(name).Update(time.Duration(v))
}

func (api *MetricsAPI) StopTimer(name string) {
	metrics.GetOrRegisterTimer(name).Stop()
}

// Percentage
func (api *MetricsAPI) MarkPercentage(name string, marked bool) {
	metrics.GetOrRegisterPercentage(name).Mark(marked)
}

// TimeWindowPercentage
func (api *MetricsAPI) MarkTimeWindowPercentageDefault(name string, marked bool) {
	metrics.GetOrRegisterTimeWindowPercentageDefault(name).Mark(marked)
}

func (api *MetricsAPI) MarkTimeWindowPercentage(name string, marked bool, slots int, slotIntervalNanos int64) {
	metrics.GetOrRegisterTimeWindowPercentage(time.Duration(slotIntervalNanos), slots, name).Mark(marked)
}
