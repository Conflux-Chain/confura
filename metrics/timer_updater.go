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

// Update updates the underlying timer metric.
func (updater *TimerUpdater) Update() {
	updater.underlying.UpdateSince(updater.start)
}
