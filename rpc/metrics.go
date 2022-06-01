package rpc

import (
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
