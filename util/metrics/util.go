package metrics

import (
	"fmt"

	"github.com/rcrowley/go-metrics"
)

func Unregister(nameFormat string, nameArgs ...any) {
	name := fmt.Sprintf(nameFormat, nameArgs...)
	metrics.DefaultRegistry.Unregister(name)
}

func GetAll() map[string]map[string]any {
	return metrics.DefaultRegistry.GetAll()
}
