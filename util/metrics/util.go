package metrics

import (
	"fmt"

	"github.com/rcrowley/go-metrics"
)

func Unregister(nameFormat string, nameArgs ...interface{}) {
	name := fmt.Sprintf(nameFormat, nameArgs...)
	metrics.DefaultRegistry.Unregister(name)
}

func GetAll() map[string]map[string]interface{} {
	return metrics.DefaultRegistry.GetAll()
}
