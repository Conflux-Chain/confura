package metrics

import (
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

// TimerUpdater is used to update timer metric with native defer syntax.
type TimerUpdater struct {
	underlying metrics.Timer
	start      time.Time
}

// NewTimerUpdater creates an instance to update timer metric.
func NewTimerUpdater(timer metrics.Timer) TimerUpdater {
	return TimerUpdater{
		underlying: timer,
		start:      time.Now(),
	}
}

// NewTimerUpdaterByName creates an instance to update timer metric
// of specified name.
func NewTimerUpdaterByName(name string) TimerUpdater {
	return TimerUpdater{
		underlying: metrics.GetOrRegisterTimer(name, nil),
		start:      time.Now(),
	}
}

// Update updates the underlying timer metric.
func (updater *TimerUpdater) Update() {
	updater.underlying.UpdateSince(updater.start)
}

// Update updates the underlying timer metric with duration.
func (updater *TimerUpdater) UpdateDuration(duration time.Duration) {
	updater.underlying.Update(duration)
}
