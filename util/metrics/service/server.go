package service

import (
	"sort"
	"time"

	"github.com/Conflux-Chain/confura/util/metrics"
	metricUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
)

const Namespace = "metrics"

type MetricsAPI struct{}

// Query APIs
func (api *MetricsAPI) List() []string {
	var names []string

	for k := range metrics.GetAll() {
		names = append(names, k)
	}

	sort.Strings(names)

	return names
}

func (api *MetricsAPI) Get(name string) map[string]any {
	return metrics.GetAll()[name]
}

func (api *MetricsAPI) All() map[string]map[string]any {
	names := api.List()

	content := make(map[string]map[string]any)

	for _, v := range names {
		content[v] = metrics.GetAll()[v]
	}

	return content
}

// Counter
func (api *MetricsAPI) ClearCounter(name string) {
	metricUtil.GetOrRegisterCounter(name).Clear()
}

func (api *MetricsAPI) DecCounter(name string, i int64) {
	metricUtil.GetOrRegisterCounter(name).Dec(i)
}

func (api *MetricsAPI) IncCounter(name string, i int64) {
	metricUtil.GetOrRegisterCounter(name).Inc(i)
}

// Gauge
func (api *MetricsAPI) UpdateGauge(name string, v int64) {
	metricUtil.GetOrRegisterGauge(name).Update(v)
}

// GaugeFloat64
func (api *MetricsAPI) UpdateGaugeFloat64(name string, v float64) {
	metricUtil.GetOrRegisterGaugeFloat64(name).Update(v)
}

// Meter
func (api *MetricsAPI) MarkMeter(name string, n int64) {
	metricUtil.GetOrRegisterMeter(name).Mark(n)
}

func (api *MetricsAPI) StopMeter(name string) {
	metricUtil.GetOrRegisterMeter(name).Stop()
}

// Histogram
func (api *MetricsAPI) ClearHistogram(name string) {
	metricUtil.GetOrRegisterHistogram(name).Clear()
}

func (api *MetricsAPI) UpdateHistogram(name string, v int64) {
	metricUtil.GetOrRegisterHistogram(name).Update(v)
}

// Timer
func (api *MetricsAPI) UpdateTimer(name string, v int64) {
	metricUtil.GetOrRegisterTimer(name).Update(time.Duration(v))
}

func (api *MetricsAPI) StopTimer(name string) {
	metricUtil.GetOrRegisterTimer(name).Stop()
}

// Percentage
func (api *MetricsAPI) MarkPercentage(name string, marked bool) {
	metricUtil.GetOrRegisterPercentage(name).Mark(marked)
}

// TimeWindowPercentage
func (api *MetricsAPI) MarkTimeWindowPercentageDefault(name string, marked bool) {
	metricUtil.GetOrRegisterTimeWindowPercentageDefault(name).Mark(marked)
}

func (api *MetricsAPI) MarkTimeWindowPercentage(name string, marked bool, slots int, slotIntervalNanos int64) {
	metricUtil.GetOrRegisterTimeWindowPercentage(time.Duration(slotIntervalNanos), slots, name).Mark(marked)
}
