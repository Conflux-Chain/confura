package handler

import (
	"context"

	"github.com/Conflux-Chain/confura/store"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	metricutil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/openweb3/web3go/client"
)

// logsQueryRetry gives all consistency conflicts the same deadline and backoff.
// Bounded requests start the budget at entry. A single-unit request may need a
// long first read; its retry budget starts only when that read is invalidated.
// Neither DB version changes nor FN conflicts reset a running budget.
type logsQueryRetry struct {
	ctx     context.Context
	cancel  context.CancelFunc
	method  string
	bounded bool
}

func newLogsQueryRetry(ctx context.Context, bounded bool, method string) *logsQueryRetry {
	r := &logsQueryRetry{ctx: ctx, method: method, bounded: bounded}
	if bounded {
		r.startBudget()
	}
	return r
}

func (r *logsQueryRetry) startBudget() {
	r.ctx, r.cancel = context.WithTimeout(r.ctx, store.TimeoutGetLogs)
}

func (r *logsQueryRetry) close() {
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *logsQueryRetry) check() error {
	if err := checkTimeout(r.ctx); err != nil {
		metricutil.GetOrRegisterMeter("infura/rpc/%v/consistency/timeout", r.method).Mark(1)
		return err
	}
	return nil
}

func (r *logsQueryRetry) retry(reason string) error {
	metricutil.GetOrRegisterMeter("infura/rpc/%v/consistency/mismatch/%v", r.method, reason).Mark(1)
	if r.cancel == nil {
		r.startBudget()
	}
	if err := waitBoundaryRetry(r.ctx); err != nil {
		return r.check()
	}
	metricutil.GetOrRegisterMeter("infura/rpc/%v/consistency/retry", r.method).Mark(1)
	return nil
}

// SDK clients are shared by the pool. Bind a copy, never mutate their context.
func cfxLogsWithContext(ctx context.Context, cfx sdk.ClientOperator) sdk.ClientOperator {
	if c, ok := cfx.(*rpcutil.CfxCoreClient); ok {
		copy := *c
		copy.ClientOperator = cfxLogsWithContext(ctx, c.ClientOperator)
		return &copy
	}
	if c, ok := cfx.(interface {
		WithContext(context.Context) *sdk.Client
	}); ok {
		return c.WithContext(ctx)
	}
	return cfx
}

func ethLogsWithContext(ctx context.Context, eth ethLogsClient) ethLogsClient {
	if c, ok := eth.(*client.RpcEthClient); ok {
		copy := *c
		copy.SetContext(ctx)
		return &copy
	}
	return eth
}
