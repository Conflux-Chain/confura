package rpc

import (
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/metrics"
)

const (
	metricPrefixRPC    = "rpc/"
	metricPrefixInfura = "infura/"
)

func init() {
	// Remove unused metrics imported from ethereum rpc package
	var names []string
	for k := range metrics.DefaultRegistry.GetAll() {
		if !strings.HasPrefix(k, metricPrefixRPC) && !strings.HasPrefix(k, metricPrefixInfura) {
			names = append(names, k)
		}
	}

	for _, v := range names {
		metrics.DefaultRegistry.Unregister(v)
	}
}

type metricsAPI struct{}

func (api *metricsAPI) List() []string {
	var names []string

	for k := range metrics.DefaultRegistry.GetAll() {
		names = append(names, k)
	}

	sort.Strings(names)

	return names
}

func (api *metricsAPI) Get(name string) map[string]interface{} {
	return metrics.DefaultRegistry.GetAll()[name]
}

func (api *metricsAPI) All() map[string]map[string]interface{} {
	names := api.List()

	content := make(map[string]map[string]interface{})

	for _, v := range names {
		content[v] = metrics.DefaultRegistry.GetAll()[v]
	}

	return content
}
