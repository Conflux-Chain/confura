package handler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	rpcutil "github.com/Conflux-Chain/confura/util/rpc"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/openweb3/go-rpc-provider/interfaces"
	web3go "github.com/openweb3/web3go"
	web3types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

func TestLogsQueryRetryBudget(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		r := newLogsQueryRetry(context.Background(), bounded, "test_getLogs")
		defer r.close()
		deadline, hasDeadline := r.ctx.Deadline()
		require.Equal(t, bounded, hasDeadline)
		if bounded {
			require.WithinDuration(t, time.Now().Add(store.TimeoutGetLogs), deadline, time.Second)
		}
		require.NoError(t, r.retry("database"))
		first, ok := r.ctx.Deadline()
		require.True(t, ok)
		if bounded {
			require.Equal(t, deadline, first)
		}
		// A different source conflict shares the same budget, beyond two retries.
		for range 3 {
			require.NoError(t, r.retry("boundary"))
			next, _ := r.ctx.Deadline()
			require.Equal(t, first, next)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := newLogsQueryRetry(ctx, false, "test_getLogs")
	defer r.close()
	cancel()
	require.ErrorIs(t, r.retry("database"), store.ErrGetLogsTimeout)
}

func TestLogsClientContextIsRequestLocal(t *testing.T) {
	provider := &logsContextTestProvider{}
	eth := web3go.NewClientWithProvider(provider)
	cfx, err := sdk.NewClientWithProvider(provider)
	require.NoError(t, err)
	cfx.SetNetworkId(1029)
	cfx.SetChainId(1029)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = ethLogsWithContext(ctx, eth.Eth).Logs(web3types.FilterQuery{})
	require.Error(t, err)
	_, err = cfxLogsWithContext(ctx, cfx).GetLogs(cfxtypes.LogFilter{})
	require.Error(t, err)
	core := rpcutil.NewCfxCoreClient(cfx)
	bound := cfxLogsWithContext(ctx, core)
	require.IsType(t, core, bound)
	_, err = bound.GetLogs(cfxtypes.LogFilter{})
	require.Error(t, err)
	_, err = core.GetLogs(cfxtypes.LogFilter{})
	require.NoError(t, err)
	// Cancelling the copies must not poison the pooled clients.
	_, err = eth.Eth.Logs(web3types.FilterQuery{})
	require.NoError(t, err)
	_, err = cfx.GetLogs(cfxtypes.LogFilter{})
	require.NoError(t, err)
}

// Exercise the real SDK context plumbing without opening a network socket.
type logsContextTestProvider struct{ interfaces.Provider }

func (*logsContextTestProvider) CallContext(ctx context.Context, result interface{}, method string, args ...interface{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return json.Unmarshal([]byte(`[]`), result)
}
