package handler

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	citypes "github.com/Conflux-Chain/confura/types"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type fakeCfxEpochNumberResolver struct {
	values map[string]uint64
	calls  []string
}

func (r *fakeCfxEpochNumberResolver) GetEpochNumber(epochs ...*cfxtypes.Epoch) (*hexutil.Big, error) {
	if len(epochs) != 1 || epochs[0] == nil {
		return nil, errors.New("expected exactly one epoch tag")
	}

	key := epochs[0].String()
	r.calls = append(r.calls, key)
	value, ok := r.values[key]
	if !ok {
		return nil, errors.Errorf("unexpected epoch tag %s", key)
	}
	return (*hexutil.Big)(new(big.Int).SetUint64(value)), nil
}

type fakeEthBlockNumberResolver struct {
	values map[web3types.BlockNumber]uint64
	calls  []web3types.BlockNumber
}

func (r *fakeEthBlockNumberResolver) BlockByNumber(
	blockNumber web3types.BlockNumber, _ bool,
) (*web3types.Block, error) {
	r.calls = append(r.calls, blockNumber)
	value, ok := r.values[blockNumber]
	if !ok {
		return nil, errors.Errorf("unexpected block tag %s", blockNumber)
	}
	return &web3types.Block{Number: new(big.Int).SetUint64(value)}, nil
}

func epochNumber(value uint64) *cfxtypes.Epoch {
	return cfxtypes.NewEpochNumberUint64(value)
}

func blockNumber(value web3types.BlockNumber) *web3types.BlockNumber {
	return &value
}

func TestNormalizeCfxScanLogRequest(t *testing.T) {
	tests := []struct {
		name      string
		from      *cfxtypes.Epoch
		to        *cfxtypes.Epoch
		values    map[string]uint64
		wantRange citypes.RangeUint64
		wantErr   error
		wantCalls map[string]int
	}{
		{
			name: "numeric range freezes latest for validation",
			from: epochNumber(10), to: epochNumber(100),
			values:    map[string]uint64{cfxtypes.EpochLatestState.String(): 100},
			wantRange: citypes.RangeUint64{From: 10, To: 100},
			wantCalls: map[string]int{cfxtypes.EpochLatestState.String(): 1},
		},
		{
			name: "numeric future upper bound",
			from: epochNumber(10), to: epochNumber(101),
			values:    map[string]uint64{cfxtypes.EpochLatestState.String(): 100},
			wantErr:   ErrScanLogsInvalidParams,
			wantCalls: map[string]int{cfxtypes.EpochLatestState.String(): 1},
		},
		{
			name: "repeated latest tag is resolved once",
			from: cfxtypes.EpochLatestState, to: cfxtypes.EpochLatestState,
			values:    map[string]uint64{cfxtypes.EpochLatestState.String(): 100},
			wantRange: citypes.RangeUint64{From: 100, To: 100},
			wantCalls: map[string]int{cfxtypes.EpochLatestState.String(): 1},
		},
		{
			name: "distinct dynamic tags are each frozen once",
			from: cfxtypes.EpochLatestConfirmed, to: cfxtypes.EpochLatestState,
			values: map[string]uint64{
				cfxtypes.EpochLatestConfirmed.String(): 90,
				cfxtypes.EpochLatestState.String():     100,
			},
			wantRange: citypes.RangeUint64{From: 90, To: 100},
			wantCalls: map[string]int{
				cfxtypes.EpochLatestConfirmed.String(): 1,
				cfxtypes.EpochLatestState.String():     1,
			},
		},
		{
			name: "reversed numeric range",
			from: epochNumber(20), to: epochNumber(10),
			wantErr: ErrScanLogsInvalidParams,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &fakeCfxEpochNumberResolver{values: test.values}
			params, err := NormalizeCfxScanLogRequest(
				resolver,
				CfxScanLogRequest{
					Filter: CfxScanLogFilter{EpochRange: &CfxEpochRange{From: test.from, To: test.to}},
					Limit:  1,
				},
				false,
			)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantRange, params.EpochRange)
			}

			actualCalls := make(map[string]int)
			for _, call := range resolver.calls {
				actualCalls[call]++
			}
			if test.wantCalls == nil {
				require.Empty(t, actualCalls)
			} else {
				require.Equal(t, test.wantCalls, actualCalls)
			}
		})
	}
}

func TestNormalizeEthScanLogRequest(t *testing.T) {
	tests := []struct {
		name      string
		from      *web3types.BlockNumber
		to        *web3types.BlockNumber
		cursor    *ScanLogCursor
		hardfork  web3types.BlockNumber
		values    map[web3types.BlockNumber]uint64
		wantRange citypes.RangeUint64
		wantErr   error
		wantCalls map[web3types.BlockNumber]int
	}{
		{
			name: "numeric range freezes latest for validation",
			from: blockNumber(10), to: blockNumber(100), hardfork: 5,
			values:    map[web3types.BlockNumber]uint64{web3types.LatestBlockNumber: 100},
			wantRange: citypes.RangeUint64{From: 10, To: 100},
			wantCalls: map[web3types.BlockNumber]int{web3types.LatestBlockNumber: 1},
		},
		{
			name: "numeric future upper bound",
			from: blockNumber(10), to: blockNumber(101), hardfork: 5,
			values:    map[web3types.BlockNumber]uint64{web3types.LatestBlockNumber: 100},
			wantErr:   ErrScanLogsInvalidParams,
			wantCalls: map[web3types.BlockNumber]int{web3types.LatestBlockNumber: 1},
		},
		{
			name:      "repeated latest tag is resolved once",
			from:      blockNumber(web3types.LatestBlockNumber),
			to:        blockNumber(web3types.LatestBlockNumber),
			values:    map[web3types.BlockNumber]uint64{web3types.LatestBlockNumber: 100},
			wantRange: citypes.RangeUint64{From: 100, To: 100},
			wantCalls: map[web3types.BlockNumber]int{web3types.LatestBlockNumber: 1},
		},
		{
			name: "distinct dynamic tags are each frozen once",
			from: blockNumber(web3types.SafeBlockNumber),
			to:   blockNumber(web3types.LatestBlockNumber),
			values: map[web3types.BlockNumber]uint64{
				web3types.SafeBlockNumber:   90,
				web3types.LatestBlockNumber: 100,
			},
			wantRange: citypes.RangeUint64{From: 90, To: 100},
			wantCalls: map[web3types.BlockNumber]int{
				web3types.SafeBlockNumber:   1,
				web3types.LatestBlockNumber: 1,
			},
		},
		{
			name: "reversed numeric range is rejected before hardfork clipping",
			from: blockNumber(9), to: blockNumber(8), hardfork: 10,
			wantErr: ErrScanLogsInvalidParams,
		},
		{
			name: "range entirely before hardfork is empty",
			from: blockNumber(1), to: blockNumber(9), hardfork: 10,
			values:    map[web3types.BlockNumber]uint64{web3types.LatestBlockNumber: 100},
			wantRange: citypes.RangeUint64(emptyScanRange),
			wantCalls: map[web3types.BlockNumber]int{web3types.LatestBlockNumber: 1},
		},
		{
			name: "range crossing hardfork starts at hardfork",
			from: blockNumber(1), to: blockNumber(11), hardfork: 10,
			values:    map[web3types.BlockNumber]uint64{web3types.LatestBlockNumber: 100},
			wantRange: citypes.RangeUint64{From: 10, To: 11},
			wantCalls: map[web3types.BlockNumber]int{web3types.LatestBlockNumber: 1},
		},
		{
			name: "cursor outside frozen range",
			from: blockNumber(10), to: blockNumber(50), hardfork: 5,
			cursor:    &ScanLogCursor{BlockNumber: 51},
			values:    map[web3types.BlockNumber]uint64{web3types.LatestBlockNumber: 100},
			wantErr:   ErrScanLogsInvalidCursor,
			wantCalls: map[web3types.BlockNumber]int{web3types.LatestBlockNumber: 1},
		},
		{
			name: "cursor inside normalized range",
			from: blockNumber(1), to: blockNumber(20), hardfork: 10,
			cursor:    &ScanLogCursor{BlockNumber: 15},
			values:    map[web3types.BlockNumber]uint64{web3types.LatestBlockNumber: 100},
			wantRange: citypes.RangeUint64{From: 10, To: 20},
			wantCalls: map[web3types.BlockNumber]int{web3types.LatestBlockNumber: 1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &fakeEthBlockNumberResolver{values: test.values}
			params, err := NormalizeEthScanLogRequest(
				resolver,
				test.hardfork,
				EthScanLogRequest{
					Filter: EthScanLogFilter{BlockRange: &EthBlockRange{From: test.from, To: test.to}},
					Limit:  1,
					Cursor: test.cursor,
				},
				false,
			)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantRange, params.BlockRange)
			}

			actualCalls := make(map[web3types.BlockNumber]int)
			for _, call := range resolver.calls {
				actualCalls[call]++
			}
			if test.wantCalls == nil {
				require.Empty(t, actualCalls)
			} else {
				require.Equal(t, test.wantCalls, actualCalls)
			}
		})
	}
}

func TestScanLogsStrictJSON(t *testing.T) {
	validHash := common.HexToHash("0x1").String()
	tests := []struct {
		name string
		data string
		dst  interface{}
	}{
		{"missing filter", `{}`, new(CfxScanLogRequest)},
		{"null filter", `{"filter":null}`, new(EthScanLogRequest)},
		{"request unknown", `{"filter":{},"blockHash":"0x1"}`, new(CfxScanLogRequest)},
		{"filter unknown", `{"filter":{"topics":[]}}`, new(EthScanLogRequest)},
		{"range unknown", `{"filter":{"epochRange":{"from":"0x1"}}}`, new(CfxScanLogRequest)},
		{"cursor unknown", `{"filter":{},"cursor":{"blockNumber":"0x1","logIndex":"0x0","source":"db"}}`, new(EthScanLogRequest)},
		{"assumption missing", `{"blockNumber":"0x1"}`, new(EthPivotAssumption)},
		{"assumption unknown", `{"blockNumber":"0x1","blockHash":"` + validHash + `","extra":true}`, new(EthPivotAssumption)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, json.Unmarshal([]byte(test.data), test.dst))
		})
	}
}

func TestScanLogsJSONQuantitiesAndFirstPageGuard(t *testing.T) {
	var req EthScanLogRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"filter":{"blockRange":{"fromBlock":"0x1","toBlock":"latest"}},
		"limit":"0x64",
		"cursor":{"blockNumber":"0xa","logIndex":"0x2"}
	}`), &req))
	require.Equal(t, hexutil.Uint64(100), req.Limit)
	require.Equal(t, hexutil.Uint64(10), req.Cursor.BlockNumber)

	log := web3types.Log{BlockNumber: 10, BlockHash: common.HexToHash("0x1234")}
	result := finishEthCandidate(
		EthScanLogParams{
			EthScanLogRequest:   &EthScanLogRequest{Limit: 1},
			WithPivotAssumption: true,
		},
		nil,
		[]web3types.Log{log},
		&ScanLogCursor{BlockNumber: 10},
		ethScanUsage{fn: true},
	)
	require.NotNil(t, result.PivotGuard)
	require.Equal(t, hexutil.Uint64(10), result.PivotGuard.BlockNumber)

	empty := finishEthCandidate(
		EthScanLogParams{
			EthScanLogRequest:   &EthScanLogRequest{Limit: 1},
			WithPivotAssumption: true,
		},
		nil,
		nil,
		nil,
		ethScanUsage{fn: true},
	)
	require.Nil(t, empty.PivotGuard)

	db := newScanLogsHandlerTestDB(t)
	pivot := common.HexToHash("0x1234")
	insertScanLogsMapping(t, db, 10, 10, 10, pivot.String())
	cfxHandler := newTestCfxScanLogsHandler(db)
	epochNumber := (*hexutil.Big)(new(big.Int).SetUint64(10))
	cfxResult, err := cfxHandler.finishCfxCandidate(
		CfxScanLogParams{
			CfxScanLogRequest:   &CfxScanLogRequest{Limit: 1},
			WithPivotAssumption: true,
		},
		nil,
		cfxScanGeneration{dbAvailable: true, dbMaxEpoch: 10},
		[]cfxtypes.Log{{EpochNumber: epochNumber}},
		&ScanLogCursor{BlockNumber: 10},
		true,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, cfxResult.PivotGuard)
	require.Equal(t, cfxtypes.Hash(pivot.String()), cfxResult.PivotGuard.PivotBlockHash)
}

func TestNormalizeScanLogsLimit(t *testing.T) {
	oldDefault, oldMax := defaultScanLogsLimit, maxScanLogsLimit
	t.Cleanup(func() {
		defaultScanLogsLimit, maxScanLogsLimit = oldDefault, oldMax
	})
	defaultScanLogsLimit, maxScanLogsLimit = 25, 50

	limit, err := normalizeScanLogsLimit(0)
	require.NoError(t, err)
	require.Equal(t, hexutil.Uint64(25), limit)
	_, err = normalizeScanLogsLimit(51)
	require.ErrorIs(t, err, ErrScanLogsInvalidParams)
}

func TestEncodeScanLogsResult(t *testing.T) {
	oldMaxBytes := maxGetLogsResponseBytes
	t.Cleanup(func() { maxGetLogsResponseBytes = oldMaxBytes })

	result := &EthScanLogResult{Logs: []web3types.Log{}}
	maxGetLogsResponseBytes = 1024
	lazy, err := EncodeScanLogsResult(result)
	require.NoError(t, err)
	expected, err := json.Marshal(result)
	require.NoError(t, err)
	actual, err := json.Marshal(lazy)
	require.NoError(t, err)
	require.Equal(t, expected, actual)

	maxGetLogsResponseBytes = 2
	_, err = EncodeScanLogsResult(result)
	require.Error(t, err)
}

type recordingScanLogsMetrics struct {
	marks       map[string]int
	percentages map[string][]bool
}

func (m *recordingScanLogsMetrics) Histogram(string, int64) {}
func (m *recordingScanLogsMetrics) Percentage(name string, marked bool) {
	if m.percentages == nil {
		m.percentages = make(map[string][]bool)
	}
	m.percentages[name] = append(m.percentages[name], marked)
}
func (m *recordingScanLogsMetrics) Duration(string, time.Time) {}
func (m *recordingScanLogsMetrics) Mark(name string)           { m.marks[name]++ }

func TestRecordScanLogsCursorOwnerPercentages(t *testing.T) {
	tests := []struct {
		name  string
		owner cursorOwner
		want  map[string][]bool
	}{
		{
			name: "none", owner: cursorOwnerNone,
			want: map[string][]bool{
				"plan/cursor_owner/none": {true},
				"plan/cursor_owner/db":   {false},
				"plan/cursor_owner/fn":   {false},
			},
		},
		{
			name: "db", owner: cursorOwnerDB,
			want: map[string][]bool{
				"plan/cursor_owner/none": {false},
				"plan/cursor_owner/db":   {true},
				"plan/cursor_owner/fn":   {false},
			},
		},
		{
			name: "fn", owner: cursorOwnerFN,
			want: map[string][]bool{
				"plan/cursor_owner/none": {false},
				"plan/cursor_owner/db":   {false},
				"plan/cursor_owner/fn":   {true},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := &recordingScanLogsMetrics{}
			ctx := withScanLogsMetrics(context.Background(), recorder)
			recordScanLogsCursorOwner(ctx, test.owner)
			require.Equal(t, test.want, recorder.percentages)
		})
	}
}

func TestScanLogsMetricsRecorderCoversWindowsAndDBCache(t *testing.T) {
	recorder := &recordingScanLogsMetrics{marks: make(map[string]int)}
	ctx := withScanLogsMetrics(context.Background(), recorder)
	reads := 0
	_, err := scanFNBlockWindows(
		ctx,
		scanRange{From: 1, To: 2},
		false,
		2,
		1,
		func(context.Context, uint64, uint64) ([]int, error) {
			reads++
			if reads == 1 {
				return nil, errors.New("the query set is too large")
			}
			return []int{1}, nil
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 2, recorder.marks["fn/window"])
	require.Equal(t, 1, recorder.marks["fn/shrink"])

	cache := &dbScanCache[int]{scan: func(
		context.Context, *store.ScanCursor, int,
	) ([]int, []store.ScanCursor, error) {
		return []int{1}, []store.ScanCursor{{BlockNumber: 1}}, nil
	}}
	require.NoError(t, cache.Ensure(ctx, 1))
	require.NoError(t, cache.Ensure(ctx, 1))
	require.Equal(t, 1, recorder.marks["db/query"])
	require.Equal(t, 1, recorder.marks["db/cache_reuse"])
}
