package metrics

import (
	"fmt"

	mutil "github.com/Conflux-Chain/go-conflux-util/metrics"
)

func Unregister(nameFormat string, nameArgs ...interface{}) {
	name := fmt.Sprintf(nameFormat, nameArgs...)
	mutil.DefaultRegistry.Unregister(name)
}

func GetAll() map[string]map[string]interface{} {
	return mutil.DefaultRegistry.GetAll()
}
