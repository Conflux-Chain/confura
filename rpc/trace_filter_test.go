package rpc

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/Conflux-Chain/confura/node"
	sdk "github.com/Conflux-Chain/go-conflux-sdk"
	cfxTypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
)

type fakeTraceEpochResolver struct {
	values map[string]uint64
	calls  map[string]int
}

// Embedding the client interface is enough for a numeric range rejected before
// any client or state handler method is invoked.
type unusedTraceClient struct{ sdk.ClientOperator }

func (r *fakeTraceEpochResolver) GetEpochNumber(epochs ...*cfxTypes.Epoch) (*hexutil.Big, error) {
	if len(epochs) != 1 {
		return nil, fmt.Errorf("expected one epoch")
	}
	key := epochs[0].String()
	r.calls[key]++
	number, ok := r.values[key]
	if !ok {
		return nil, fmt.Errorf("unknown epoch %s", key)
	}
	return (*hexutil.Big)(new(big.Int).SetUint64(number)), nil
}

type fakeTraceBlockResolver struct {
	values map[ethTypes.BlockNumber]uint64
	calls  map[ethTypes.BlockNumber]int
}

func (r *fakeTraceBlockResolver) Resolve(block ethTypes.BlockNumber) (uint64, error) {
	r.calls[block]++
	number, ok := r.values[block]
	if !ok {
		return 0, fmt.Errorf("unknown block %s", block)
	}
	return number, nil
}

func traceEpochNumber(number uint64) *cfxTypes.Epoch {
	return cfxTypes.NewEpochNumberUint64(number)
}

func traceBlockNumber(number ethTypes.BlockNumber) *ethTypes.BlockNumber {
	return &number
}

func TestValidateCfxTraceFilterRange(t *testing.T) {
	latest := cfxTypes.EpochLatestState.String()
	confirmed := cfxTypes.EpochLatestConfirmed.String()
	tests := []struct {
		name    string
		filter  cfxTypes.TraceFilter
		values  map[string]uint64
		wantErr string
		calls   map[string]int
	}{
		{name: "one epoch", filter: cfxTypes.TraceFilter{FromEpoch: traceEpochNumber(1), ToEpoch: traceEpochNumber(1)}},
		{name: "1000 epochs", filter: cfxTypes.TraceFilter{FromEpoch: traceEpochNumber(1), ToEpoch: traceEpochNumber(1000)}},
		{name: "1001 epochs", filter: cfxTypes.TraceFilter{FromEpoch: traceEpochNumber(1), ToEpoch: traceEpochNumber(1001)}, wantErr: "maximum of 1000 epochs"},
		{name: "reverse range", filter: cfxTypes.TraceFilter{FromEpoch: traceEpochNumber(10), ToEpoch: traceEpochNumber(9)}, wantErr: "invalid epoch range"},
		{name: "missing bounds skips resolution"},
		{name: "missing lower bound", filter: cfxTypes.TraceFilter{ToEpoch: traceEpochNumber(1200)}, values: map[string]uint64{latest: 1200}, calls: map[string]int{latest: 1}},
		{name: "missing upper bound", filter: cfxTypes.TraceFilter{FromEpoch: traceEpochNumber(201)}, values: map[string]uint64{latest: 1200}, calls: map[string]int{latest: 1}},
		{name: "distinct tags", filter: cfxTypes.TraceFilter{FromEpoch: cfxTypes.EpochLatestConfirmed, ToEpoch: cfxTypes.EpochLatestState}, values: map[string]uint64{confirmed: 201, latest: 1200}, calls: map[string]int{confirmed: 1, latest: 1}},
		{name: "tag resolution failure", filter: cfxTypes.TraceFilter{FromEpoch: cfxTypes.EpochLatestState}, wantErr: "failed to resolve epoch", calls: map[string]int{latest: 1}},
		{name: "epoch over uint64", filter: cfxTypes.TraceFilter{FromEpoch: cfxTypes.NewEpochNumberBig(new(big.Int).Lsh(big.NewInt(1), 64))}, wantErr: "out of range"},
		{name: "uint64 boundary", filter: cfxTypes.TraceFilter{FromEpoch: traceEpochNumber(math.MaxUint64 - 999), ToEpoch: traceEpochNumber(math.MaxUint64)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &fakeTraceEpochResolver{values: test.values, calls: make(map[string]int)}
			err := validateCfxTraceFilter(resolver, test.filter)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, resolver.calls, len(test.calls))
			for key, count := range test.calls {
				require.Equal(t, count, resolver.calls[key])
			}
		})
	}
}

func TestValidateCfxTraceFilterBlockHashes(t *testing.T) {
	for _, test := range []struct {
		name    string
		hashes  int
		wantErr bool
	}{
		{name: "empty array", hashes: 0},
		{name: "64 hashes", hashes: 64},
		{name: "65 hashes", hashes: 65, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			filter := cfxTypes.TraceFilter{
				BlockHashes: make([]cfxTypes.Hash, test.hashes),
				FromEpoch:   traceEpochNumber(1),
				ToEpoch:     traceEpochNumber(1001),
			}
			resolver := &fakeTraceEpochResolver{calls: make(map[string]int)}
			err := validateCfxTraceFilter(resolver, filter)
			if test.wantErr {
				require.ErrorContains(t, err, "maximum of 64 hashes")
			} else {
				require.NoError(t, err)
			}
			require.Empty(t, resolver.calls)
		})
	}
}

func TestValidateEthTraceFilterRange(t *testing.T) {
	latest := ethTypes.LatestBlockNumber
	tests := []struct {
		name    string
		filter  ethTypes.TraceFilter
		values  map[ethTypes.BlockNumber]uint64
		wantErr string
		calls   map[ethTypes.BlockNumber]int
	}{
		{name: "one block", filter: ethTypes.TraceFilter{FromBlock: traceBlockNumber(1), ToBlock: traceBlockNumber(1)}},
		{name: "1000 blocks", filter: ethTypes.TraceFilter{FromBlock: traceBlockNumber(1), ToBlock: traceBlockNumber(1000)}},
		{name: "1001 blocks", filter: ethTypes.TraceFilter{FromBlock: traceBlockNumber(1), ToBlock: traceBlockNumber(1001)}, wantErr: "maximum of 1000 blocks"},
		{name: "reverse range", filter: ethTypes.TraceFilter{FromBlock: traceBlockNumber(10), ToBlock: traceBlockNumber(9)}, wantErr: "invalid block range"},
		{name: "missing bounds skips resolution"},
		{name: "missing lower bound", filter: ethTypes.TraceFilter{ToBlock: traceBlockNumber(1200)}, values: map[ethTypes.BlockNumber]uint64{latest: 1200}, calls: map[ethTypes.BlockNumber]int{latest: 1}},
		{name: "missing upper bound", filter: ethTypes.TraceFilter{FromBlock: traceBlockNumber(201)}, values: map[ethTypes.BlockNumber]uint64{latest: 1200}, calls: map[ethTypes.BlockNumber]int{latest: 1}},
		{name: "tag resolution failure", filter: ethTypes.TraceFilter{ToBlock: traceBlockNumber(1200)}, wantErr: "failed to resolve block", calls: map[ethTypes.BlockNumber]int{latest: 1}},
		{name: "resolved number is only checked", filter: ethTypes.TraceFilter{FromBlock: &latest, ToBlock: &latest}, values: map[ethTypes.BlockNumber]uint64{latest: math.MaxInt64 + 1}, calls: map[ethTypes.BlockNumber]int{latest: 1}},
		{name: "int64 boundary", filter: ethTypes.TraceFilter{FromBlock: traceBlockNumber(math.MaxInt64 - 999), ToBlock: traceBlockNumber(math.MaxInt64)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &fakeTraceBlockResolver{values: test.values, calls: make(map[ethTypes.BlockNumber]int)}
			err := validateEthTraceFilter(resolver, test.filter)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, resolver.calls, len(test.calls))
			for key, count := range test.calls {
				require.Equal(t, count, resolver.calls[key])
			}
		})
	}
}

func TestTraceFilterRejectsOversizedRangeBeforeUpstreamRequest(t *testing.T) {
	cfxCtx := context.WithValue(context.Background(), ctxKeyClient, &unusedTraceClient{})
	_, err := (&traceAPI{}).Filter(cfxCtx, cfxTypes.TraceFilter{
		FromEpoch: traceEpochNumber(1),
		ToEpoch:   traceEpochNumber(1001),
	})
	require.ErrorContains(t, err, "maximum of 1000 epochs")

	ethCtx := context.WithValue(context.Background(), ctxKeyClient, &node.Web3goClient{})
	_, err = (&ethTraceAPI{}).Filter(ethCtx, ethTypes.TraceFilter{
		FromBlock: traceBlockNumber(1),
		ToBlock:   traceBlockNumber(1001),
	})
	require.ErrorContains(t, err, "maximum of 1000 blocks")
}
