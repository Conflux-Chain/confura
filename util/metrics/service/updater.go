package service

// TODO implements Updater interface
// 1. Implement as a RPC client.
// 2. Batch RPC every 100 milliseconds or number of metrics exceeds 1000.

type Updater interface {
	ClearCounter(name string)
	DecCounter(name string, i int64)
	IncCounter(name string, i int64)

	UpdateGauge(name string, v int64)
	DecGauge(name string, i int64)
	IncGauge(name string, i int64)

	UpdateGaugeFloat64(name string, v float64)

	MarkMeter(name string, n int64)
	StopMeter(name string)

	ClearHistogram(name string)
	UpdateHistogram(name string, v int64)

	UpdateTimer(name string, v int64)
	StopTimer(name string)

	MarkPercentage(name string, marked bool)

	MarkTimeWindowPercentageDefault(name string, marked bool)
	MarkTimeWindowPercentage(name string, marked bool, slots int, slotIntervalNanos int64)
}
