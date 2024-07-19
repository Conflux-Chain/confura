package service

import (
	"time"

	metricUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/ethereum/go-ethereum/metrics"
)

type clientMetric struct {
	name    string // metric name to update on server side
	updater Updater
}

// Counter
type Counter struct {
	clientMetric
	metrics.Counter
}

func (counter *Counter) Clear() {
	counter.Counter.Clear()
	counter.updater.ClearCounter(counter.name)
}

func (counter *Counter) Dec(i int64) {
	counter.Counter.Dec(i)
	counter.updater.DecCounter(counter.name, i)
}

func (counter *Counter) Inc(i int64) {
	counter.Counter.Inc(i)
	counter.updater.IncCounter(counter.name, i)
}

// Gauge
type Gauge struct {
	clientMetric
	metrics.Gauge
}

func (gauge *Gauge) Update(v int64) {
	gauge.Gauge.Update(v)
	gauge.updater.UpdateGauge(gauge.name, v)
}

func (gauge *Gauge) Dec(i int64) {
	gauge.Gauge.Dec(i)
	gauge.updater.DecGauge(gauge.name, i)
}

func (gauge *Gauge) Inc(i int64) {
	gauge.Gauge.Inc(i)
	gauge.updater.IncGauge(gauge.name, i)
}

// GaugeFloat64
type GaugeFloat64 struct {
	clientMetric
	metrics.GaugeFloat64
}

func (gauge *GaugeFloat64) Update(v float64) {
	gauge.GaugeFloat64.Update(v)
	gauge.updater.UpdateGaugeFloat64(gauge.name, v)
}

// Meter
type Meter struct {
	clientMetric
	metrics.Meter
}

func (meter *Meter) Mark(n int64) {
	meter.Meter.Mark(n)
	meter.updater.MarkMeter(meter.name, n)
}

func (meter *Meter) Stop() {
	meter.Meter.Stop()
	meter.updater.StopMeter(meter.name)
}

// Histogram
type Histogram struct {
	clientMetric
	metrics.Histogram
}

func (h *Histogram) Clear() {
	h.Histogram.Clear()
	h.updater.ClearHistogram(h.name)
}

func (h *Histogram) Update(v int64) {
	h.Histogram.Update(v)
	h.updater.UpdateHistogram(h.name, v)
}

// Timer
type Timer struct {
	clientMetric
	metrics.Timer
}

func (timer *Timer) Time(f func()) {
	start := time.Now()
	f()
	timer.UpdateSince(start)
}

func (timer *Timer) Update(d time.Duration) {
	timer.Timer.Update(d)
	timer.updater.UpdateTimer(timer.name, d.Nanoseconds())
}

func (timer *Timer) UpdateSince(ts time.Time) {
	timer.Update(time.Since(ts))
}

func (timer *Timer) Stop() {
	timer.Timer.Stop()
	timer.updater.StopTimer(timer.name)
}

// Percentage
type Percentage struct {
	clientMetric
	metricUtil.Percentage
}

func (p *Percentage) Mark(marked bool) {
	p.Percentage.Mark(marked)
	p.updater.MarkPercentage(p.name, marked)
}

// TimeWindowPercentage
type TimeWindowPercentage struct {
	clientMetric
	metricUtil.Percentage
	slots    int
	interval time.Duration
}

func (p *TimeWindowPercentage) Mark(marked bool) {
	p.Percentage.Mark(marked)
	p.updater.MarkTimeWindowPercentage(p.name, marked, p.slots, p.interval.Nanoseconds())
}
