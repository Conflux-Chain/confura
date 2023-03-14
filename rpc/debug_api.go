package rpc

import (
	"context"

	"github.com/Conflux-Chain/confura/util/metrics"
)

// debugAPI provides several non-standard RPC methods, which provide some run time diagnostics
// such as topK traffic hits etc. for inspection and debugging.
type debugAPI struct{}

func (api *debugAPI) TopkStats(ctx context.Context, k int) ([]metrics.Visitor, error) {
	return metrics.DefaultTrafficCollector().TopkVisitors(k), nil
}
