package service

import (
	"sort"

	"github.com/conflux-chain/conflux-infura/util/metrics"
)

const Namespace = "metrics"

type MetricsAPI struct{}

func (api *MetricsAPI) List() []string {
	var names []string

	for k := range metrics.InfuraRegistry.GetAll() {
		names = append(names, k)
	}

	sort.Strings(names)

	return names
}

func (api *MetricsAPI) Get(name string) map[string]interface{} {
	return metrics.InfuraRegistry.GetAll()[name]
}

func (api *MetricsAPI) All() map[string]map[string]interface{} {
	names := api.List()

	content := make(map[string]map[string]interface{})

	for _, v := range names {
		content[v] = metrics.InfuraRegistry.GetAll()[v]
	}

	return content
}
