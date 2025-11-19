package service

import (
	"fmt"
	"time"

	metricUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/rcrowley/go-metrics"
)

type Registry struct {
	metrics.Registry
	updater Updater
}

func NewRegistry(updater Updater) metrics.Registry {
	return &Registry{
		Registry: metrics.NewRegistry(),
		updater:  updater,
	}
}

func (r *Registry) GetOrRegisterCounter(name string, args ...any) metrics.Counter {
	metricName := fmt.Sprintf(name, args...)
	return r.Registry.GetOrRegister(name, func() metrics.Counter {
		return &Counter{clientMetric{metricName, r.updater}, metrics.NewCounter()}
	}).(metrics.Counter)
}

func (r *Registry) GetOrRegisterGauge(name string, args ...any) metrics.Gauge {
	metricName := fmt.Sprintf(name, args...)
	return r.Registry.GetOrRegister(name, func() metrics.Gauge {
		return &Gauge{clientMetric{metricName, r.updater}, metrics.NewGauge()}
	}).(metrics.Gauge)
}

func (r *Registry) GetOrRegisterGaugeFloat64(name string, args ...any) metrics.GaugeFloat64 {
	metricName := fmt.Sprintf(name, args...)
	return r.Registry.GetOrRegister(name, func() metrics.GaugeFloat64 {
		return &GaugeFloat64{clientMetric{metricName, r.updater}, metrics.NewGaugeFloat64()}
	}).(metrics.GaugeFloat64)
}

func (r *Registry) GetOrRegisterMeter(name string, args ...any) metrics.Meter {
	metricName := fmt.Sprintf(name, args...)
	return r.Registry.GetOrRegister(name, func() metrics.Meter {
		return &Meter{clientMetric{metricName, r.updater}, metrics.NewMeter()}
	}).(metrics.Meter)
}

func (r *Registry) GetOrRegisterHistogram(name string, args ...any) metrics.Histogram {
	metricName := fmt.Sprintf(name, args...)
	return r.Registry.GetOrRegister(name, func() metrics.Histogram {
		return &Histogram{
			clientMetric{metricName, r.updater},
			metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)),
		}
	}).(metrics.Histogram)
}

func (r *Registry) GetOrRegisterTimer(name string, args ...any) metrics.Timer {
	metricName := fmt.Sprintf(name, args...)
	return r.Registry.GetOrRegister(name, func() metrics.Timer {
		return &Timer{clientMetric{metricName, r.updater}, metrics.NewTimer()}
	}).(metrics.Timer)
}

func (r *Registry) GetOrRegisterPercentage(name string, args ...any) metricUtil.Percentage {
	metricName := fmt.Sprintf(name, args...)
	return r.Registry.GetOrRegister(name, func() metricUtil.Percentage {
		return &Percentage{clientMetric{metricName, r.updater}, metricUtil.NewPercentage()}
	}).(metricUtil.Percentage)
}

func (r *Registry) GetOrRegisterTimeWindowPercentageDefault(name string, args ...any) metricUtil.Percentage {
	return r.GetOrRegisterTimeWindowPercentage(10, time.Minute, name, args...)
}

func (r *Registry) GetOrRegisterTimeWindowPercentage(slots int, slotInterval time.Duration, name string, args ...any) metricUtil.Percentage {
	metricName := fmt.Sprintf(name, args...)
	return r.Registry.GetOrRegister(name, func() metricUtil.Percentage {
		return &TimeWindowPercentage{clientMetric{metricName, r.updater}, metricUtil.NewPercentage(), slots, slotInterval}
	}).(metricUtil.Percentage)
}
