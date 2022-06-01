package metrics

import (
	"fmt"

	"github.com/ethereum/go-ethereum/metrics"
)

// Note, must use metrics.DefaultRegistry from geth, since go-rpc-provider depends on it
// for rpc metrics by default. When RPC middleware supported at server side, we can use
// a custom metrics registry.
var InfuraRegistry = metrics.DefaultRegistry

func GetOrRegisterCounter(nameFormat string, nameArgs ...interface{}) metrics.Counter {
	name := fmt.Sprintf(nameFormat, nameArgs...)
	return metrics.GetOrRegisterCounter(name, InfuraRegistry)
}

func GetOrRegisterGauge(nameFormat string, nameArgs ...interface{}) metrics.Gauge {
	name := fmt.Sprintf(nameFormat, nameArgs...)
	return metrics.GetOrRegisterGauge(name, InfuraRegistry)
}

func GetOrRegisterGaugeFloat64(nameFormat string, nameArgs ...interface{}) metrics.GaugeFloat64 {
	name := fmt.Sprintf(nameFormat, nameArgs...)
	return metrics.GetOrRegisterGaugeFloat64(name, InfuraRegistry)
}

func GetOrRegisterMeter(nameFormat string, nameArgs ...interface{}) metrics.Meter {
	name := fmt.Sprintf(nameFormat, nameArgs...)
	return metrics.GetOrRegisterMeter(name, InfuraRegistry)
}

func NewHistogram() metrics.Histogram {
	return metrics.NewHistogram(metrics.NewExpDecaySample(1024, 0.015))
}

func GetOrRegisterHistogram(nameFormat string, nameArgs ...interface{}) metrics.Histogram {
	name := fmt.Sprintf(nameFormat, nameArgs...)
	return InfuraRegistry.GetOrRegister(name, NewHistogram).(metrics.Histogram)
}

func GetOrRegisterTimer(nameFormat string, nameArgs ...interface{}) metrics.Timer {
	name := fmt.Sprintf(nameFormat, nameArgs...)
	return metrics.GetOrRegisterTimer(name, InfuraRegistry)
}
