package metrics

import (
	"fmt"

	"github.com/ethereum/go-ethereum/metrics"
)

func GetOrRegisterHistogram(r metrics.Registry, nameFormat string, nameArgs ...interface{}) metrics.Histogram {
	name := fmt.Sprintf(nameFormat, nameArgs...)
	return metrics.GetOrRegisterHistogram(name, r, metrics.NewExpDecaySample(1024, 0.015))
}
