package handler

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	citypes "github.com/Conflux-Chain/confura/types"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestBuildScanPlanPlacesCursorOnce(t *testing.T) {
	cursor := &store.ScanCursor{BlockNumber: 20, LogIndex: 3}
	tests := []struct {
		name    string
		reverse bool
		owner   cursorOwner
		want    []scanSegment
	}{
		{"forward without cursor", false, cursorOwnerNone, []scanSegment{{source: scanSourceDB}, {source: scanSourceFN}}},
		{"reverse without cursor", true, cursorOwnerNone, []scanSegment{{source: scanSourceFN}, {source: scanSourceDB}}},
		{"forward DB cursor", false, cursorOwnerDB, []scanSegment{{source: scanSourceDB, cursor: cursor}, {source: scanSourceFN}}},
		{"reverse DB cursor", true, cursorOwnerDB, []scanSegment{{source: scanSourceDB, cursor: cursor}}},
		{"forward FN cursor", false, cursorOwnerFN, []scanSegment{{source: scanSourceFN, cursor: cursor}}},
		{"reverse FN cursor", true, cursorOwnerFN, []scanSegment{{source: scanSourceFN, cursor: cursor}, {source: scanSourceDB}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := buildScanPlan(true, true, test.reverse, test.owner, cursor)
			require.Len(t, plan.segments, len(test.want))
			for i := range test.want {
				assert.Equal(t, test.want[i].source, plan.segments[i].source)
				assert.Equal(t, test.want[i].cursor, plan.segments[i].cursor)
			}
			cursorCopies := 0
			for _, segment := range plan.segments {
				if segment.cursor != nil {
					cursorCopies++
				}
			}
			if test.owner == cursorOwnerNone {
				assert.Zero(t, cursorCopies)
			} else {
				assert.Equal(t, 1, cursorCopies)
			}
		})
	}
}

func newScanLogsHandlerTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)

	require.NoError(t, db.Exec(`
		CREATE TABLE configs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			value TEXT NOT NULL,
			created_at DATETIME,
			updated_at DATETIME
		)
	`).Error)
	require.NoError(t, db.Exec(`
		CREATE TABLE epoch_block_map (
			epoch INTEGER PRIMARY KEY,
			bn_min INTEGER NOT NULL,
			bn_max INTEGER NOT NULL,
			pivot_hash TEXT NOT NULL
		)
	`).Error)
	return db
}

func insertScanLogsMapping(t *testing.T, db *gorm.DB, epoch, bnMin, bnMax uint64, pivot string) {
	t.Helper()
	require.NoError(t, db.Exec(
		"INSERT INTO epoch_block_map (epoch, bn_min, bn_max, pivot_hash) VALUES (?, ?, ?, ?)",
		epoch, bnMin, bnMax, pivot,
	).Error)
}

func insertEthScanLogsTestPartition(t *testing.T, db *gorm.DB, block uint64) {
	t.Helper()
	require.NoError(t, db.Exec(`
		CREATE TABLE bn_partitions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			entity TEXT NOT NULL,
			pi INTEGER NOT NULL,
			count INTEGER NOT NULL,
			bn_min INTEGER,
			bn_max INTEGER,
			created_at DATETIME,
			updated_at DATETIME
		)
	`).Error)
	require.NoError(t, db.Exec(`
		CREATE TABLE logs_0 (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bn INTEGER NOT NULL,
			epoch INTEGER NOT NULL,
			topic0 TEXT NOT NULL,
			topic1 TEXT,
			topic2 TEXT,
			topic3 TEXT,
			log_index INTEGER NOT NULL,
			extra BLOB
		)
	`).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO bn_partitions (entity, pi, count, bn_min, bn_max) VALUES ('logs', 0, 1, ?, ?)", block, block,
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO logs_0 (bn, epoch, topic0, log_index, extra) VALUES (?, ?, '', 0, '{}')", block, block,
	).Error)
}

func newTestCfxScanLogsHandler(db *gorm.DB) *CfxLogsApiHandler {
	return NewCfxLogsApiHandler(
		mysql.NewCfxStore(db, &mysql.Config{}, store.StoreConfig()), nil, 0,
	)
}

func newTestEthScanLogsHandler(db *gorm.DB) *EthLogsApiHandler {
	return NewEthLogsApiHandler(
		mysql.NewEthStore(db, &mysql.Config{}, store.EthStoreConfig()), 0,
	)
}

func TestScanLogsErrorsUseFrameworkDefaultCode(t *testing.T) {
	type codedError interface{ ErrorCode() int }

	categories := []error{
		ErrScanLogsUnavailable,
		ErrScanLogsInvalidParams,
		ErrScanLogsInvalidCursor,
		ErrScanLogsConsistency,
		ErrScanLogsAssumptionFailure,
	}
	for _, category := range categories {
		t.Run(category.Error(), func(t *testing.T) {
			cause := errors.New("concrete reason")
			err := NewScanLogsError(category, cause)

			require.EqualError(t, err, category.Error()+": concrete reason")
			require.ErrorIs(t, err, category)
			require.ErrorIs(t, err, cause)
			assert.Same(t, cause, errors.Cause(err))
			_, custom := err.(codedError)
			assert.False(t, custom)
		})
	}

	canonical := newCanonicalDependentError(
		ErrScanLogsAssumptionFailure, "assumption validation failed",
	)
	require.EqualError(t, canonical, "pivot assumption failed: assumption validation failed")
	require.ErrorIs(t, canonical, ErrScanLogsAssumptionFailure)
	assert.True(t, isCanonicalDependentError(canonical))
	require.EqualError(t, errors.Cause(canonical), "assumption validation failed")
	_, custom := canonical.(codedError)
	assert.False(t, custom)
}

func TestClassifyCursorOwnerRejectsCursorOutsideRequestDBRange(t *testing.T) {
	dbRange := scanRange{From: 100, To: 150}
	owner, err := classifyCursorOwner(&store.ScanCursor{BlockNumber: 90}, dbRange, 200)
	require.EqualError(t, err, "cursor 90 is outside the block range [100, 150] of the split DB segment")
	require.NotErrorIs(t, err, ErrScanLogsInvalidCursor)
	assert.Equal(t, cursorOwnerNone, owner)

	owner, err = classifyCursorOwner(&store.ScanCursor{BlockNumber: 120}, dbRange, 200)
	require.NoError(t, err)
	assert.Equal(t, cursorOwnerDB, owner)

	owner, err = classifyCursorOwner(&store.ScanCursor{BlockNumber: 201}, dbRange, 200)
	require.NoError(t, err)
	assert.Equal(t, cursorOwnerFN, owner)
}

func TestMissingCfxMappingsBuildsPureFullnodePlan(t *testing.T) {
	cursor := &store.ScanCursor{BlockNumber: 5009, LogIndex: 2}
	gen := newCfxFullnodeGeneration(scanRange{From: 100, To: 200}, cursor, false)

	assert.False(t, gen.dbAvailable)
	assert.True(t, gen.dbEpochs.empty())
	assert.True(t, gen.dbBlocks.empty())
	assert.Equal(t, scanRange{From: 100, To: 200}, gen.fnEpochs)
	assert.Equal(t, cursorOwnerFN, gen.owner)
	require.Len(t, gen.plan.segments, 1)
	assert.Equal(t, scanSourceFN, gen.plan.segments[0].source)
	assert.Equal(t, cursor, gen.plan.segments[0].cursor)
}

func TestMissingEthMappingsBuildsPureFullnodePlan(t *testing.T) {
	cursor := &store.ScanCursor{BlockNumber: 150, LogIndex: 3}
	gen := newEthFullnodeGeneration(scanRange{From: 100, To: 200}, cursor, true)

	assert.False(t, gen.dbAvailable)
	assert.True(t, gen.dbBlocks.empty())
	assert.Equal(t, scanRange{From: 100, To: 200}, gen.fnBlocks)
	assert.Equal(t, scanRange{From: 100, To: 200}, gen.requestBlocks)
	assert.Equal(t, cursorOwnerFN, gen.owner)
	require.Len(t, gen.plan.segments, 1)
	assert.Equal(t, scanSourceFN, gen.plan.segments[0].source)
	assert.Equal(t, cursor, gen.plan.segments[0].cursor)
}

func TestBuildEthGenerationRejectsFNCursorWhenFNRangeIsEmpty(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`
		CREATE TABLE epoch_block_map (
			epoch INTEGER PRIMARY KEY,
			bn_min INTEGER NOT NULL,
			bn_max INTEGER NOT NULL,
			pivot_hash TEXT NOT NULL
		)
	`).Error)

	handler := NewEthLogsApiHandler(
		mysql.NewEthStore(db, &mysql.Config{}, store.EthStoreConfig()), 0,
	)

	t.Run("empty mapping table", func(t *testing.T) {
		_, err := handler.buildEthGeneration(EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{
				Limit: 1, Cursor: &ScanLogCursor{BlockNumber: 101},
			},
			BlockRange: citypes.RangeUint64{From: 100, To: 99},
		})
		require.ErrorIs(t, err, ErrScanLogsInvalidCursor)
	})

	require.NoError(t, db.Exec(`
		INSERT INTO epoch_block_map (epoch, bn_min, bn_max, pivot_hash)
		VALUES (900, 0, 900, '0x0900')
	`).Error)

	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("DB-covered request reverse=%t", reverse), func(t *testing.T) {
			_, err := handler.buildEthGeneration(EthScanLogParams{
				EthScanLogRequest: &EthScanLogRequest{
					Limit: 1, Cursor: &ScanLogCursor{BlockNumber: 1000}, Reverse: reverse,
				},
				BlockRange: citypes.RangeUint64{From: 100, To: 800},
			})
			require.ErrorIs(t, err, ErrScanLogsInvalidCursor)
		})
	}
}

func TestBuildEthInnerCandidateReturnsInvalidCursorWithoutFence(t *testing.T) {
	req := EthScanLogParams{
		EthScanLogRequest: &EthScanLogRequest{
			Limit:  1,
			Cursor: &ScanLogCursor{BlockNumber: 101, LogIndex: 2},
		},
	}
	gen := newEthFullnodeGeneration(scanRange{From: 100, To: 99}, req.Cursor.toStoreCursor(), false)
	_, err := (&EthLogsApiHandler{}).buildEthInnerCandidate(
		context.Background(),
		&fakeEthScanClient{},
		req,
		nil,
		ethOuterState{gen: gen},
		99,
	)

	require.ErrorIs(t, err, ErrScanLogsInvalidCursor)
	assert.False(t, isCanonicalDependentError(err))
}

func TestBuildEthInnerCandidateKeepsUnmetAssumptionProvisional(t *testing.T) {
	assumption := &EthPivotAssumption{
		BlockNumber: 150,
		BlockHash:   common.HexToHash("0x150a"),
	}
	client := &fakeEthScanClient{blocks: map[int64]*web3types.Block{
		150: {Number: big.NewInt(150), Hash: common.HexToHash("0x150b")},
	}}
	candidate, err := (&EthLogsApiHandler{}).buildEthInnerCandidate(
		context.Background(),
		client,
		EthScanLogParams{EthScanLogRequest: &EthScanLogRequest{Limit: 1}},
		assumption,
		ethOuterState{
			gen:          newEthFullnodeGeneration(scanRange{From: 100, To: 99}, nil, false),
			fnAssumption: true,
		},
		150,
	)

	require.NoError(t, err)
	require.ErrorIs(t, candidate.err, ErrScanLogsAssumptionFailure)
	assert.True(t, isCanonicalDependentError(candidate.err))
	assert.True(t, candidate.usage.fn)
	assert.NotNil(t, candidate.result)
}

func TestDBScanCacheIncrementallyUsesCachedTail(t *testing.T) {
	var calls []struct {
		cursor *store.ScanCursor
		limit  int
	}
	all := []uint64{9, 8, 7, 6, 5}
	cache := dbScanCache[uint64]{
		scan: func(_ context.Context, cursor *store.ScanCursor, limit int) ([]uint64, []store.ScanCursor, error) {
			calls = append(calls, struct {
				cursor *store.ScanCursor
				limit  int
			}{cloneScanCursor(cursor), limit})
			start := 0
			if cursor != nil {
				for i, value := range all {
					if value == cursor.BlockNumber {
						start = i + 1
					}
				}
			}
			end := start + limit
			if end > len(all) {
				end = len(all)
			}
			logs := append([]uint64(nil), all[start:end]...)
			keys := make([]store.ScanCursor, len(logs))
			for i, value := range logs {
				keys[i] = store.ScanCursor{BlockNumber: value}
			}
			return logs, keys, nil
		},
	}
	require.NoError(t, cache.Ensure(context.Background(), 2))
	require.NoError(t, cache.Ensure(context.Background(), 4))
	require.NoError(t, cache.Ensure(context.Background(), 3))
	assert.Equal(t, []uint64{9, 8, 7, 6}, cache.logs)
	require.Len(t, calls, 2)
	assert.Nil(t, calls[0].cursor)
	assert.Equal(t, 2, calls[0].limit)
	assert.Equal(t, &store.ScanCursor{BlockNumber: 8}, calls[1].cursor)
	assert.Equal(t, 2, calls[1].limit)
	assert.Equal(t, &store.ScanCursor{BlockNumber: 7}, cache.Tail(3))
}

func TestDBScanCacheRemembersExhaustion(t *testing.T) {
	calls := 0
	cache := dbScanCache[int]{
		scan: func(context.Context, *store.ScanCursor, int) ([]int, []store.ScanCursor, error) {
			calls++
			return nil, nil, nil
		},
	}
	require.NoError(t, cache.Ensure(context.Background(), 3))
	require.NoError(t, cache.Ensure(context.Background(), 5))
	assert.Equal(t, 1, calls)
	assert.True(t, cache.exhausted)
}

func TestCanonicalCommitDecisionKeepsFNRetryInsideDBGeneration(t *testing.T) {
	tests := []struct {
		name             string
		dbStable         bool
		checkpointStable bool
		boundaryAligned  bool
		want             canonicalCommitDecision
	}{
		{"all stable", true, true, true, canonicalCommit},
		{"checkpoint changed", true, false, true, canonicalRetryInner},
		{"boundary mismatch", true, true, false, canonicalRetryInner},
		{"DB changed dominates checkpoint", false, false, true, canonicalRetryOuter},
		{"DB changed dominates boundary", false, true, false, canonicalRetryOuter},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, decideCanonicalCommit(test.dbStable, test.checkpointStable, test.boundaryAligned))
		})
	}
}

type fakeCfxScanClient struct {
	byHash          map[cfxtypes.Hash]*cfxtypes.BlockSummary
	byNumber        map[uint64]*cfxtypes.BlockSummary
	byEpoch         map[uint64]*cfxtypes.BlockSummary
	logs            []cfxtypes.Log
	byHashFn        func(cfxtypes.Hash) (*cfxtypes.BlockSummary, error)
	byNumberFn      func(uint64) (*cfxtypes.BlockSummary, error)
	byEpochFn       func(uint64) (*cfxtypes.BlockSummary, error)
	getLogsFn       func(cfxtypes.LogFilter) ([]cfxtypes.Log, error)
	byHashCalls     int
	byNumberCalls   int
	byEpochCalls    int
	byEpochRequests []uint64
	getLogsFilters  []cfxtypes.LogFilter
}

func (f *fakeCfxScanClient) GetBlockSummaryByHash(hash cfxtypes.Hash) (*cfxtypes.BlockSummary, error) {
	f.byHashCalls++
	if f.byHashFn != nil {
		return f.byHashFn(hash)
	}
	return f.byHash[hash], nil
}
func (f *fakeCfxScanClient) GetBlockSummaryByEpoch(epoch *cfxtypes.Epoch) (*cfxtypes.BlockSummary, error) {
	f.byEpochCalls++
	value, _ := epoch.ToInt()
	f.byEpochRequests = append(f.byEpochRequests, value.Uint64())
	if f.byEpochFn != nil {
		return f.byEpochFn(value.Uint64())
	}
	return f.byEpoch[value.Uint64()], nil
}
func (f *fakeCfxScanClient) GetBlockSummaryByBlockNumber(number hexutil.Uint64) (*cfxtypes.BlockSummary, error) {
	f.byNumberCalls++
	if f.byNumberFn != nil {
		return f.byNumberFn(uint64(number))
	}
	return f.byNumber[uint64(number)], nil
}
func (f *fakeCfxScanClient) GetLogs(filter cfxtypes.LogFilter) ([]cfxtypes.Log, error) {
	f.getLogsFilters = append(f.getLogsFilters, filter)
	if f.getLogsFn != nil {
		return f.getLogsFn(filter)
	}
	return append([]cfxtypes.Log(nil), f.logs...), nil
}

func TestCfxScanLogsPublicEntryCommitsStableDBError(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	insertScanLogsMapping(t, db, 10, 100, 100, "0x01")
	handler := newTestCfxScanLogsHandler(db)

	client := &fakeCfxScanClient{}
	_, err := handler.ScanLogs(
		context.Background(),
		client,
		CfxScanLogParams{
			CfxScanLogRequest: &CfxScanLogRequest{Limit: 1},
			EpochRange:        citypes.RangeUint64{From: 10, To: 10},
		},
		&CfxPivotAssumption{EpochNumber: 10, PivotBlockHash: cfxtypes.Hash("0x02")},
	)

	require.ErrorIs(t, err, ErrScanLogsAssumptionFailure)
	assert.Zero(t, client.byHashCalls)
	assert.Zero(t, client.byNumberCalls)
	assert.Zero(t, client.byEpochCalls)
	assert.Empty(t, client.getLogsFilters)
}

func TestCfxScanLogsPublicEntryRetriesChangedCheckpoint(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	handler := newTestCfxScanLogsHandler(db)

	hashes := []cfxtypes.Hash{"0x0a", "0x0b", "0x0c", "0x0c"}
	next := 0
	client := &fakeCfxScanClient{byEpochFn: func(epoch uint64) (*cfxtypes.BlockSummary, error) {
		require.Equal(t, uint64(0), epoch)
		require.Less(t, next, len(hashes))
		summary := cfxSummary(hashes[next], 0, 0)
		next++
		return summary, nil
	}}

	result, err := handler.ScanLogs(
		context.Background(),
		client,
		CfxScanLogParams{
			CfxScanLogRequest: &CfxScanLogRequest{Limit: 1},
			EpochRange:        citypes.RangeUint64{From: 0, To: 0},
		},
		nil,
	)

	require.NoError(t, err)
	assert.Empty(t, result.Logs)
	assert.Equal(t, 4, client.byEpochCalls)
	assert.Equal(t, []uint64{0, 0, 0, 0}, client.byEpochRequests)
	assert.Len(t, client.getLogsFilters, 2, "changed checkpoint must replay the FN segment")
}

func TestCfxScanLogsRetriesCursorEpochMismatchFromChangedCheckpoint(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	handler := newTestCfxScanLogsHandler(db)

	checkpointHashes := []cfxtypes.Hash{"0x10a", "0x10b", "0x10c", "0x10c"}
	checkpointCalls := 0
	client := &fakeCfxScanClient{
		byEpochFn: func(epoch uint64) (*cfxtypes.BlockSummary, error) {
			if epoch == 104 {
				return cfxSummary("0x104", epoch, 1050), nil
			}
			require.Equal(t, uint64(110), epoch)
			require.Less(t, checkpointCalls, len(checkpointHashes))
			summary := cfxSummary(checkpointHashes[checkpointCalls], epoch, 1100)
			checkpointCalls++
			return summary, nil
		},
		byNumberFn: func(number uint64) (*cfxtypes.BlockSummary, error) {
			require.Equal(t, uint64(1000), number)
			if clientCall := checkpointCalls; clientCall == 1 {
				return cfxSummary("0x1000a", 105, number), nil
			}
			return cfxSummary("0x1000b", 103, number), nil
		},
	}

	result, err := handler.ScanLogs(
		context.Background(),
		client,
		CfxScanLogParams{
			CfxScanLogRequest: &CfxScanLogRequest{
				Limit: 1,
				Cursor: &ScanLogCursor{
					BlockNumber: 1000,
				},
			},
			EpochRange: citypes.RangeUint64{From: 100, To: 104},
		},
		&CfxPivotAssumption{EpochNumber: 110, PivotBlockHash: "0x10c"},
	)

	require.NoError(t, err)
	assert.Empty(t, result.Logs)
	assert.Equal(t, 4, checkpointCalls)
	assert.Equal(t, 2, client.byNumberCalls)
	assert.Len(t, client.getLogsFilters, 1, "the invalid first view must retry before scanning logs")
}

func TestCfxScanLogsCommitsStableCursorEpochMismatch(t *testing.T) {
	handler := newTestCfxScanLogsHandler(newScanLogsHandlerTestDB(t))
	checkpointCalls := 0
	client := &fakeCfxScanClient{
		byEpochFn: func(epoch uint64) (*cfxtypes.BlockSummary, error) {
			require.Equal(t, uint64(110), epoch)
			checkpointCalls++
			return cfxSummary("0x110", epoch, 1100), nil
		},
		byNumber: map[uint64]*cfxtypes.BlockSummary{
			1000: cfxSummary("0x1000", 105, 1000),
		},
	}

	_, err := handler.ScanLogs(
		context.Background(),
		client,
		CfxScanLogParams{
			CfxScanLogRequest: &CfxScanLogRequest{
				Limit: 1,
				Cursor: &ScanLogCursor{
					BlockNumber: 1000,
				},
			},
			EpochRange: citypes.RangeUint64{From: 100, To: 104},
		},
		&CfxPivotAssumption{EpochNumber: 110, PivotBlockHash: "0x110"},
	)

	require.ErrorIs(t, err, ErrScanLogsInvalidCursor)
	assert.Equal(t, 2, checkpointCalls, "stable invalid cursor must close the FN fence before publication")
	assert.Equal(t, 1, client.byNumberCalls)
	assert.Empty(t, client.getLogsFilters)
}

func TestCfxScanLogsBoundaryCanonicalErrorClosesFence(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	insertScanLogsMapping(t, db, 0, 0, 0, "0x10")
	handler := newTestCfxScanLogsHandler(db)

	hCalls, boundaryCalls := 0, 0
	client := &fakeCfxScanClient{byEpochFn: func(epoch uint64) (*cfxtypes.BlockSummary, error) {
		switch epoch {
		case 1:
			hCalls++
			return cfxSummary(cfxtypes.Hash("0x11"), 1, 1), nil
		case 0:
			boundaryCalls++
			// A lower epoch resolving above H is canonical-view dependent. It must
			// remain provisional until the second H read and DB v1 both complete.
			return cfxSummary(cfxtypes.Hash("0x12"), 0, 2), nil
		default:
			t.Fatalf("unexpected epoch lookup %d", epoch)
			return nil, nil
		}
	}}

	_, err := handler.ScanLogs(
		context.Background(),
		client,
		CfxScanLogParams{
			CfxScanLogRequest: &CfxScanLogRequest{Limit: 1},
			EpochRange:        citypes.RangeUint64{From: 1, To: 1},
		},
		&CfxPivotAssumption{EpochNumber: 0, PivotBlockHash: cfxtypes.Hash("0x10")},
	)

	require.ErrorIs(t, err, ErrScanLogsConsistency)
	assert.Equal(t, 2, hCalls, "canonical boundary error must still close the FN before/after fence")
	assert.Equal(t, 1, boundaryCalls)
}

func TestBuildCfxGenerationRejectsInvalidEndpointMapping(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	insertScanLogsMapping(t, db, 10, 101, 100, "0x10")
	handler := newTestCfxScanLogsHandler(db)

	_, err := handler.buildCfxGeneration(CfxScanLogParams{
		CfxScanLogRequest: &CfxScanLogRequest{Limit: 1},
		EpochRange:        citypes.RangeUint64{From: 10, To: 10},
	})
	require.ErrorIs(t, err, ErrScanLogsConsistency)
}

func TestCfxRouteBReaderFiltersOnlyCursorBlock(t *testing.T) {
	const epoch = uint64(1004)
	b5009, b5010, checkpointHash := cfxtypes.Hash("0x5009"), cfxtypes.Hash("0x5010"), cfxtypes.Hash("0x5015")
	client := &fakeCfxScanClient{
		byHash: map[cfxtypes.Hash]*cfxtypes.BlockSummary{b5010: cfxSummary(b5010, epoch, 5010)},
		logs: []cfxtypes.Log{
			cfxLog(b5009, epoch, 1),
			cfxLog(b5009, epoch, 4),
			cfxLog(b5010, epoch, 0),
		},
	}
	attempt, err := newCfxFNAttemptView(client, epoch+1, cfxSummary(checkpointHash, epoch+1, 5015), nil)
	require.NoError(t, err)
	cursor := &store.ScanCursor{BlockNumber: 5009, LogIndex: 2}
	reader := cfxFNReader{client: client, attempt: attempt, spec: cfxFNReaderSpec{
		blocks: scanRange{From: 5009, To: 5015}, cursor: cursor, cursorHash: &b5009, windowSize: 10,
	}}
	batch, err := reader.Scan(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, batch.Logs, 2)
	assert.Equal(t, b5009, *batch.Logs[0].BlockHash)
	assert.Equal(t, uint64(4), batch.Logs[0].LogIndex.ToInt().Uint64())
	assert.Equal(t, b5010, *batch.Logs[1].BlockHash)
	assert.Equal(t, &store.ScanCursor{BlockNumber: 5010, LogIndex: 0}, batch.TailPosition)
	require.Len(t, client.getLogsFilters, 1)
	assert.Equal(t, uint64(5009), client.getLogsFilters[0].FromBlock.ToInt().Uint64())
	assert.Nil(t, client.getLogsFilters[0].FromEpoch)
}

func TestCfxRouteBReaderKeepsLaterBlockWhenCursorBlockHasNoLogs(t *testing.T) {
	const epoch = uint64(1004)
	b5009, b5010, checkpointHash := cfxtypes.Hash("0x5009"), cfxtypes.Hash("0x5010"), cfxtypes.Hash("0x5015")
	client := &fakeCfxScanClient{
		byHash: map[cfxtypes.Hash]*cfxtypes.BlockSummary{b5010: cfxSummary(b5010, epoch, 5010)},
		logs:   []cfxtypes.Log{cfxLog(b5010, epoch, 0)},
	}
	attempt, err := newCfxFNAttemptView(client, epoch+1, cfxSummary(checkpointHash, epoch+1, 5015), nil)
	require.NoError(t, err)
	reader := cfxFNReader{client: client, attempt: attempt, spec: cfxFNReaderSpec{
		blocks: scanRange{From: 5009, To: 5015}, cursor: &store.ScanCursor{BlockNumber: 5009, LogIndex: 2},
		cursorHash: &b5009, windowSize: 10,
	}}
	batch, err := reader.Scan(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, batch.Logs, 1)
	assert.Equal(t, b5010, *batch.Logs[0].BlockHash)
}

func TestCfxRouteBReaderReverseFiltersCursorBlock(t *testing.T) {
	const epoch = uint64(1004)
	b5008, b5009, checkpointHash := cfxtypes.Hash("0x5008"), cfxtypes.Hash("0x5009"), cfxtypes.Hash("0x5015")
	client := &fakeCfxScanClient{
		byHash: map[cfxtypes.Hash]*cfxtypes.BlockSummary{b5008: cfxSummary(b5008, epoch, 5008)},
		logs: []cfxtypes.Log{
			cfxLog(b5008, epoch, 5), cfxLog(b5009, epoch, 1),
			cfxLog(b5009, epoch, 4),
		},
	}
	attempt, err := newCfxFNAttemptView(client, epoch+1, cfxSummary(checkpointHash, epoch+1, 5015), nil)
	require.NoError(t, err)
	reader := cfxFNReader{client: client, attempt: attempt, spec: cfxFNReaderSpec{
		blocks: scanRange{From: 5000, To: 5009}, cursor: &store.ScanCursor{BlockNumber: 5009, LogIndex: 3},
		cursorHash: &b5009, reverse: true, windowSize: 10,
	}}
	batch, err := reader.Scan(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, batch.Logs, 2)
	assert.Equal(t, b5009, *batch.Logs[0].BlockHash)
	assert.Equal(t, uint64(1), batch.Logs[0].LogIndex.ToInt().Uint64())
	assert.Equal(t, b5008, *batch.Logs[1].BlockHash)
	assert.Equal(t, &store.ScanCursor{BlockNumber: 5008, LogIndex: 5}, batch.TailPosition)
}

func TestCfxFNReaderRejectsIncompleteLogsBeforeCursorFiltering(t *testing.T) {
	tests := []struct {
		name    string
		missing string
		mutate  func(*cfxtypes.Log)
	}{
		{
			name: "block hash", missing: "missing block hash",
			mutate: func(log *cfxtypes.Log) { log.BlockHash = nil },
		},
		{
			name: "epoch number", missing: "missing epoch number",
			mutate: func(log *cfxtypes.Log) { log.EpochNumber = nil },
		},
		{
			name: "log index", missing: "missing log index",
			mutate: func(log *cfxtypes.Log) { log.LogIndex = nil },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const epoch = uint64(100)
			blockHash := cfxtypes.Hash("0x5000")
			log := cfxLog(blockHash, epoch, 1)
			test.mutate(&log)

			client := &fakeCfxScanClient{logs: []cfxtypes.Log{log}}
			attempt, err := newCfxFNAttemptView(
				client, epoch, cfxSummary("0x5010", epoch, 5010), nil,
			)
			require.NoError(t, err)
			reader := cfxFNReader{client: client, attempt: attempt, spec: cfxFNReaderSpec{
				blocks: scanRange{From: 5000, To: 5000},
				cursor: &store.ScanCursor{BlockNumber: 5000}, cursorHash: &blockHash,
				windowSize: 10,
			}}

			_, err = reader.Scan(context.Background(), 10)

			require.Error(t, err)
			assert.ErrorContains(t, err, test.missing)
		})
	}
}

func TestCfxRouteBBlockPlanReusesCheckpointAndCursorBoundary(t *testing.T) {
	const checkpointEpoch = uint64(110)
	cursorHash, checkpointHash := cfxtypes.Hash("0x5009"), cfxtypes.Hash("0x5100")
	client := &fakeCfxScanClient{byNumber: map[uint64]*cfxtypes.BlockSummary{
		5009: cfxSummary(cursorHash, 105, 5009),
	}}
	attempt, err := newCfxFNAttemptView(client, checkpointEpoch, cfxSummary(checkpointHash, checkpointEpoch, 5100), nil)
	require.NoError(t, err)
	plan, err := attempt.resolveBlockPlan(
		cfxScanGeneration{fnEpochs: scanRange{From: 100, To: checkpointEpoch}},
		scanSegment{source: scanSourceFN, cursor: &store.ScanCursor{BlockNumber: 5009, LogIndex: 2}},
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, scanRange{From: 5009, To: 5100}, plan.blocks)
	require.NotNil(t, plan.cursorHash)
	assert.Equal(t, cursorHash, *plan.cursorHash)
	assert.Equal(t, 1, client.byNumberCalls)
	assert.Zero(t, client.byEpochCalls, "checkpoint and forward cursor replace both range endpoint RPCs")
}

func TestCfxRouteBPureFNFirstPageUsesPreviousPivotOnce(t *testing.T) {
	const fromEpoch, checkpointEpoch = uint64(100), uint64(110)
	previousHash, checkpointHash := cfxtypes.Hash("0x4900"), cfxtypes.Hash("0x5100")
	client := &fakeCfxScanClient{byEpoch: map[uint64]*cfxtypes.BlockSummary{
		fromEpoch - 1: cfxSummary(previousHash, fromEpoch-1, 4900),
	}}
	attempt, err := newCfxFNAttemptView(client, checkpointEpoch, cfxSummary(checkpointHash, checkpointEpoch, 5100), nil)
	require.NoError(t, err)
	plan, err := attempt.resolveBlockPlan(
		cfxScanGeneration{fnEpochs: scanRange{From: fromEpoch, To: checkpointEpoch}},
		scanSegment{source: scanSourceFN},
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, scanRange{From: 4901, To: 5100}, plan.blocks)
	assert.Equal(t, 1, client.byEpochCalls)
}

func TestCfxRouteBMixedPageUsesDBBoundaryWithoutRangeRPC(t *testing.T) {
	const checkpointEpoch = uint64(110)
	checkpointHash := cfxtypes.Hash("0x5100")
	client := &fakeCfxScanClient{}
	attempt, err := newCfxFNAttemptView(client, checkpointEpoch, cfxSummary(checkpointHash, checkpointEpoch, 5100), nil)
	require.NoError(t, err)
	plan, err := attempt.resolveBlockPlan(cfxScanGeneration{
		dbAvailable: true,
		dbMaxEpoch:  99,
		dbMaxBlock:  4900,
		fnEpochs:    scanRange{From: 100, To: checkpointEpoch},
	}, scanSegment{source: scanSourceFN}, false)
	require.NoError(t, err)
	assert.Equal(t, scanRange{From: 4901, To: 5100}, plan.blocks)
	assert.Zero(t, client.byEpochCalls)
}

func TestCfxRouteBReverseCursorResolvesOnlyLowerBoundary(t *testing.T) {
	const fromEpoch, checkpointEpoch = uint64(100), uint64(110)
	previousHash, cursorHash, checkpointHash := cfxtypes.Hash("0x4900"), cfxtypes.Hash("0x5009"), cfxtypes.Hash("0x5100")
	client := &fakeCfxScanClient{
		byEpoch: map[uint64]*cfxtypes.BlockSummary{
			fromEpoch - 1: cfxSummary(previousHash, fromEpoch-1, 4900),
		},
		byNumber: map[uint64]*cfxtypes.BlockSummary{
			5009: cfxSummary(cursorHash, 105, 5009),
		},
	}
	attempt, err := newCfxFNAttemptView(client, checkpointEpoch, cfxSummary(checkpointHash, checkpointEpoch, 5100), nil)
	require.NoError(t, err)
	plan, err := attempt.resolveBlockPlan(
		cfxScanGeneration{fnEpochs: scanRange{From: fromEpoch, To: checkpointEpoch}},
		scanSegment{source: scanSourceFN, cursor: &store.ScanCursor{BlockNumber: 5009, LogIndex: 2}},
		true,
	)
	require.NoError(t, err)
	assert.Equal(t, scanRange{From: 4901, To: 5009}, plan.blocks)
	assert.Equal(t, 1, client.byEpochCalls)
	assert.Equal(t, 1, client.byNumberCalls)
}

func TestCfxRouteBResolvesToEpochWhenCheckpointIsHigher(t *testing.T) {
	const fromEpoch, toEpoch, checkpointEpoch = uint64(100), uint64(110), uint64(120)
	client := &fakeCfxScanClient{byEpoch: map[uint64]*cfxtypes.BlockSummary{
		fromEpoch - 1: cfxSummary(cfxtypes.Hash("0x4900"), fromEpoch-1, 4900),
		toEpoch:       cfxSummary(cfxtypes.Hash("0x5100"), toEpoch, 5100),
	}}
	attempt, err := newCfxFNAttemptView(
		client, checkpointEpoch, cfxSummary(cfxtypes.Hash("0x5200"), checkpointEpoch, 5200), nil,
	)
	require.NoError(t, err)
	plan, err := attempt.resolveBlockPlan(
		cfxScanGeneration{fnEpochs: scanRange{From: fromEpoch, To: toEpoch}},
		scanSegment{source: scanSourceFN},
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, scanRange{From: 4901, To: 5100}, plan.blocks)
	assert.Equal(t, 2, client.byEpochCalls)
}

func TestCfxRouteBKeepsCursorAboveCheckpointProvisional(t *testing.T) {
	const checkpointEpoch = uint64(110)
	client := &fakeCfxScanClient{}
	attempt, err := newCfxFNAttemptView(
		client, checkpointEpoch, cfxSummary(cfxtypes.Hash("0x5100"), checkpointEpoch, 5100), nil,
	)
	require.NoError(t, err)
	_, err = attempt.resolveBlockPlan(
		cfxScanGeneration{fnEpochs: scanRange{From: 100, To: checkpointEpoch}},
		scanSegment{source: scanSourceFN, cursor: &store.ScanCursor{BlockNumber: 5101}},
		false,
	)
	require.ErrorIs(t, err, ErrScanLogsInvalidCursor)
	assert.True(t, isCanonicalDependentError(err))
	assert.Zero(t, client.byNumberCalls)
}

func TestCfxRouteBKeepsCursorOutsideEpochRangeProvisional(t *testing.T) {
	const checkpointEpoch = uint64(110)
	client := &fakeCfxScanClient{byNumber: map[uint64]*cfxtypes.BlockSummary{
		1000: cfxSummary(cfxtypes.Hash("0x1000"), 105, 1000),
	}}
	attempt, err := newCfxFNAttemptView(
		client, checkpointEpoch, cfxSummary(cfxtypes.Hash("0x1100"), checkpointEpoch, 1100), nil,
	)
	require.NoError(t, err)
	_, err = attempt.resolveBlockPlan(
		cfxScanGeneration{fnEpochs: scanRange{From: 100, To: 104}},
		scanSegment{source: scanSourceFN, cursor: &store.ScanCursor{BlockNumber: 1000}},
		false,
	)
	require.ErrorIs(t, err, ErrScanLogsInvalidCursor)
	assert.True(t, isCanonicalDependentError(err))
}

func TestCfxRouteBTrustsBlockSummaryLookupContract(t *testing.T) {
	const checkpointEpoch = uint64(110)
	client := &fakeCfxScanClient{byNumber: map[uint64]*cfxtypes.BlockSummary{
		5009: cfxSummary(cfxtypes.Hash("0x5010"), 105, 5010),
	}}
	attempt, err := newCfxFNAttemptView(
		client, checkpointEpoch, cfxSummary(cfxtypes.Hash("0x5100"), checkpointEpoch, 5100), nil,
	)
	require.NoError(t, err)
	plan, err := attempt.resolveBlockPlan(
		cfxScanGeneration{fnEpochs: scanRange{From: 100, To: checkpointEpoch}},
		scanSegment{source: scanSourceFN, cursor: &store.ScanCursor{BlockNumber: 5009}},
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, scanRange{From: 5009, To: 5100}, plan.blocks)
	require.NotNil(t, plan.cursorHash)
	assert.Equal(t, cfxtypes.Hash("0x5010"), *plan.cursorHash)
}

func TestBuildCfxInnerCandidateChecksCheckpointAssumptionHash(t *testing.T) {
	const epoch = uint64(150)
	realHash := cfxtypes.Hash("0x150b")
	client := &fakeCfxScanClient{}
	attempt, err := newCfxFNAttemptView(client, epoch, cfxSummary(realHash, epoch, 6000), nil)
	require.NoError(t, err)
	assumption := &CfxPivotAssumption{EpochNumber: hexutil.Uint64(epoch), PivotBlockHash: cfxtypes.Hash("0x150a")}
	candidate, err := (&CfxLogsApiHandler{}).buildCfxInnerCandidate(
		context.Background(),
		client,
		CfxScanLogParams{CfxScanLogRequest: &CfxScanLogRequest{Limit: 1}},
		assumption,
		cfxOuterState{
			gen:          newCfxFullnodeGeneration(scanRange{From: 100, To: 99}, nil, false),
			fnAssumption: true,
		},
		attempt,
	)
	require.NoError(t, err)
	require.ErrorIs(t, candidate.err, ErrScanLogsAssumptionFailure)
	assert.True(t, isCanonicalDependentError(candidate.err))
	assert.True(t, candidate.usage.fn)
}

type fakeEthScanClient struct {
	blocks       map[int64]*web3types.Block
	logs         []web3types.Log
	blockFn      func(int64) (*web3types.Block, error)
	logsFn       func(web3types.FilterQuery) ([]web3types.Log, error)
	blockCalls   int
	blockNumbers []int64
	filters      []web3types.FilterQuery
}

func (f *fakeEthScanClient) BlockByNumber(number web3types.BlockNumber, _ bool) (*web3types.Block, error) {
	f.blockCalls++
	f.blockNumbers = append(f.blockNumbers, int64(number))
	if f.blockFn != nil {
		return f.blockFn(int64(number))
	}
	return f.blocks[int64(number)], nil
}

func (f *fakeEthScanClient) Logs(filter web3types.FilterQuery) ([]web3types.Log, error) {
	f.filters = append(f.filters, filter)
	if f.logsFn != nil {
		return f.logsFn(filter)
	}
	return append([]web3types.Log(nil), f.logs...), nil
}

func TestEthScanLogsPublicEntryCommitsStableDBError(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	insertScanLogsMapping(t, db, 10, 10, 10, common.HexToHash("0x01").String())
	handler := newTestEthScanLogsHandler(db)

	client := &fakeEthScanClient{}
	_, err := handler.ScanLogs(
		context.Background(),
		client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 1},
			BlockRange:        citypes.RangeUint64{From: 10, To: 10},
		},
		&EthPivotAssumption{BlockNumber: 10, BlockHash: common.HexToHash("0x02")},
	)

	require.ErrorIs(t, err, ErrScanLogsAssumptionFailure)
	assert.Zero(t, client.blockCalls)
	assert.Empty(t, client.filters)
}

func TestEthScanLogsPublicEntryRetriesChangedCheckpoint(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	handler := newTestEthScanLogsHandler(db)

	hashes := []common.Hash{
		common.HexToHash("0x0a"), common.HexToHash("0x0b"),
		common.HexToHash("0x0c"), common.HexToHash("0x0c"),
	}
	next := 0
	client := &fakeEthScanClient{blockFn: func(number int64) (*web3types.Block, error) {
		require.Equal(t, int64(0), number)
		require.Less(t, next, len(hashes))
		block := &web3types.Block{Number: big.NewInt(0), Hash: hashes[next]}
		next++
		return block, nil
	}}

	result, err := handler.ScanLogs(
		context.Background(),
		client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 1},
			BlockRange:        citypes.RangeUint64{From: 0, To: 0},
		},
		nil,
	)

	require.NoError(t, err)
	assert.Empty(t, result.Logs)
	assert.Equal(t, 4, client.blockCalls)
	assert.Equal(t, []int64{0, 0, 0, 0}, client.blockNumbers)
	assert.Len(t, client.filters, 2, "changed checkpoint must replay the FN segment")
}

func TestEthScanLogsRepeatedCheckpointChangeRejectsCandidate(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	dbHash := common.HexToHash("0x10")
	insertScanLogsMapping(t, db, 0, 0, 0, dbHash.String())
	insertEthScanLogsTestPartition(t, db, 0)

	hashes := []common.Hash{
		common.HexToHash("0x11a"), common.HexToHash("0x11b"),
		common.HexToHash("0x11c"), common.HexToHash("0x11d"),
	}
	hCalls := 0
	client := &fakeEthScanClient{blockFn: func(number int64) (*web3types.Block, error) {
		switch number {
		case 0:
			return &web3types.Block{Number: big.NewInt(number), Hash: dbHash}, nil
		case 1:
			hash := hashes[hCalls]
			hCalls++
			return &web3types.Block{Number: big.NewInt(number), Hash: hash}, nil
		default:
			t.Fatalf("unexpected block lookup %d", number)
			return nil, nil
		}
	}}

	result, err := newTestEthScanLogsHandler(db).ScanLogs(
		context.Background(), client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 2},
			BlockRange:        citypes.RangeUint64{From: 0, To: 1},
		},
		nil,
	)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrScanLogsConsistency)
	assert.Equal(t, 4, hCalls)
	assert.Len(t, client.filters, 2)
}

func TestEthScanLogsCheckpointTransportErrorReturnsImmediately(t *testing.T) {
	wantErr := errors.New("checkpoint transport failed")
	tests := []struct {
		name       string
		failOnCall int
		wantCalls  int
		wantScans  int
	}{
		{name: "before", failOnCall: 1, wantCalls: 1, wantScans: 0},
		{name: "after", failOnCall: 2, wantCalls: 2, wantScans: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeEthScanClient{blockFn: func(number int64) (*web3types.Block, error) {
				if test.failOnCall == 1 || test.failOnCall == 2 {
					test.failOnCall--
					if test.failOnCall == 0 {
						return nil, wantErr
					}
				}
				return &web3types.Block{Number: big.NewInt(number), Hash: common.HexToHash("0x10")}, nil
			}}

			result, err := newTestEthScanLogsHandler(newScanLogsHandlerTestDB(t)).ScanLogs(
				context.Background(), client,
				EthScanLogParams{
					EthScanLogRequest: &EthScanLogRequest{Limit: 1},
					BlockRange:        citypes.RangeUint64{From: 0, To: 0},
				},
				nil,
			)

			assert.Nil(t, result)
			assert.ErrorIs(t, err, wantErr)
			assert.Equal(t, test.wantCalls, client.blockCalls)
			assert.Len(t, client.filters, test.wantScans)
		})
	}
}

func TestEthScanLogsOuterRetryRebuildsInnerStateAndDBCache(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	dbHash := common.HexToHash("0x10")
	fnHashA, fnHashB := common.HexToHash("0x11a"), common.HexToHash("0x11b")
	insertScanLogsMapping(t, db, 0, 0, 0, dbHash.String())
	insertEthScanLogsTestPartition(t, db, 0)
	require.NoError(t, db.Exec(
		"INSERT INTO configs (name, value) VALUES ('reorg.version', '1')",
	).Error)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	configReads, logReads := 0, 0
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register(
		"scanlogs_test_change_version", func(tx *gorm.DB) {
			if tx.Statement.Table != "configs" {
				return
			}
			configReads++
			if configReads == 3 {
				_, updateErr := sqlDB.Exec("UPDATE configs SET value = '2' WHERE name = 'reorg.version'")
				require.NoError(t, updateErr)
			}
		},
	))
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(
		"scanlogs_test_count_log_reads", func(tx *gorm.DB) {
			if tx.Statement.Table == "logs_0" {
				logReads++
			}
		},
	))

	hCalls, scanCalls := 0, 0
	client := &fakeEthScanClient{blockFn: func(number int64) (*web3types.Block, error) {
		if number == 0 {
			return &web3types.Block{Number: big.NewInt(number), Hash: dbHash}, nil
		}
		hCalls++
		hash := fnHashB
		if hCalls == 1 {
			hash = fnHashA
		}
		return &web3types.Block{Number: big.NewInt(number), Hash: hash}, nil
	}, logsFn: func(web3types.FilterQuery) ([]web3types.Log, error) {
		scanCalls++
		return []web3types.Log{{
			BlockNumber: 1, BlockHash: common.BigToHash(big.NewInt(int64(scanCalls))), Index: 0,
		}}, nil
	}}

	result, err := newTestEthScanLogsHandler(db).ScanLogs(
		context.Background(), client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 2},
			BlockRange:        citypes.RangeUint64{From: 0, To: 1},
		},
		nil,
	)

	require.NoError(t, err)
	require.Len(t, result.Logs, 2)
	assert.Equal(t, common.BigToHash(big.NewInt(3)), result.Logs[1].BlockHash,
		"only the candidate from the second outer generation may be committed")
	assert.Equal(t, 3, len(client.filters), "one inner retry plus one rebuilt outer attempt")
	assert.Equal(t, 2, logReads, "DB cache is reused by inner retry and rebuilt by outer retry")
	assert.Equal(t, 5, configReads)
}

func TestEthScanLogsRetryKeepsNormalizedLatestFrozen(t *testing.T) {
	hashes := []common.Hash{
		common.HexToHash("0x1a"), common.HexToHash("0x1b"),
		common.HexToHash("0x1c"), common.HexToHash("0x1c"),
	}
	hashIndex := 0
	client := &fakeEthScanClient{blockFn: func(number int64) (*web3types.Block, error) {
		if number == int64(web3types.LatestBlockNumber) {
			return &web3types.Block{Number: big.NewInt(1), Hash: common.HexToHash("0xhead")}, nil
		}
		require.Equal(t, int64(1), number)
		hash := hashes[hashIndex]
		hashIndex++
		return &web3types.Block{Number: big.NewInt(number), Hash: hash}, nil
	}}

	params, err := NormalizeEthScanLogRequest(client, 0, EthScanLogRequest{Limit: 1}, false)
	require.NoError(t, err)
	result, err := newTestEthScanLogsHandler(newScanLogsHandlerTestDB(t)).ScanLogs(
		context.Background(), client, params, nil,
	)

	require.NoError(t, err)
	assert.Empty(t, result.Logs)
	assert.Equal(t, []int64{int64(web3types.LatestBlockNumber), 1, 1, 1, 1}, client.blockNumbers)
	assert.Len(t, client.filters, 2)
}

func TestEthScanLogsDBResultWithFNAssumptionValidatesBoundary(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	dbHash := common.HexToHash("0x10")
	assumptionHash := common.HexToHash("0x11")
	insertScanLogsMapping(t, db, 0, 0, 0, dbHash.String())
	insertEthScanLogsTestPartition(t, db, 0)

	client := &fakeEthScanClient{blockFn: func(number int64) (*web3types.Block, error) {
		switch number {
		case 0:
			return &web3types.Block{Number: big.NewInt(number), Hash: dbHash}, nil
		case 1:
			return &web3types.Block{Number: big.NewInt(number), Hash: assumptionHash}, nil
		default:
			t.Fatalf("unexpected block lookup %d", number)
			return nil, nil
		}
	}}
	result, err := newTestEthScanLogsHandler(db).ScanLogs(
		context.Background(),
		client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 2},
			BlockRange:        citypes.RangeUint64{From: 0, To: 0},
		},
		&EthPivotAssumption{BlockNumber: 1, BlockHash: assumptionHash},
	)

	require.NoError(t, err)
	require.Len(t, result.Logs, 1)
	assert.Equal(t, uint64(0), result.Logs[0].BlockNumber)
	assert.Equal(t, []int64{1, 1, 0, 1}, client.blockNumbers)
	assert.Empty(t, client.filters, "the FN is used only for the out-of-DB assumption")
}

func TestEthScanLogsRejectsNullPreCheckpointBlock(t *testing.T) {
	handler := newTestEthScanLogsHandler(newScanLogsHandlerTestDB(t))
	client := &fakeEthScanClient{}

	_, err := handler.ScanLogs(
		context.Background(),
		client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 1},
			BlockRange:        citypes.RangeUint64{From: 10, To: 10},
		},
		nil,
	)

	require.ErrorIs(t, err, ErrScanLogsConsistency)
	require.EqualError(t, err, "inconsistent canonical views: pre-checkpoint block 10 is unavailable")
	assert.Equal(t, 1, client.blockCalls)
	assert.Empty(t, client.filters, "the FN segment must not run without an opening fence")
}

func TestEthScanLogsRetriesNullPostCheckpointBlock(t *testing.T) {
	handler := newTestEthScanLogsHandler(newScanLogsHandlerTestDB(t))
	checkpointHash := common.HexToHash("0x10")
	client := &fakeEthScanClient{}
	client.blockFn = func(number int64) (*web3types.Block, error) {
		require.Equal(t, int64(10), number)
		if client.blockCalls == 2 {
			return nil, nil
		}
		return &web3types.Block{Number: big.NewInt(number), Hash: checkpointHash}, nil
	}

	result, err := handler.ScanLogs(
		context.Background(),
		client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 1},
			BlockRange:        citypes.RangeUint64{From: 10, To: 10},
		},
		nil,
	)

	require.NoError(t, err)
	assert.Empty(t, result.Logs)
	assert.Equal(t, 4, client.blockCalls)
	assert.Len(t, client.filters, 2, "a null post-checkpoint block must replay the FN segment")
}

func TestEthScanLogsRetriesNullBoundaryBlockThenRejects(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	insertScanLogsMapping(t, db, 10, 10, 10, common.HexToHash("0x10").String())
	handler := newTestEthScanLogsHandler(db)
	client := &fakeEthScanClient{blockFn: func(number int64) (*web3types.Block, error) {
		switch number {
		case 11:
			return &web3types.Block{Number: big.NewInt(number), Hash: common.HexToHash("0x11")}, nil
		case 10:
			return nil, nil
		default:
			t.Fatalf("unexpected block lookup %d", number)
			return nil, nil
		}
	}}

	_, err := handler.ScanLogs(
		context.Background(),
		client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 1},
			BlockRange:        citypes.RangeUint64{From: 11, To: 11},
		},
		&EthPivotAssumption{BlockNumber: 10, BlockHash: common.HexToHash("0x10")},
	)

	require.ErrorIs(t, err, ErrScanLogsConsistency)
	assert.Equal(t, 6, client.blockCalls)
	assert.Len(t, client.filters, 2, "a null boundary block permits one additional FN attempt")
}

func TestEthScanLogsBoundaryMismatchConvergesOnRetry(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	dbHash := common.HexToHash("0x10")
	insertScanLogsMapping(t, db, 0, 0, 0, dbHash.String())
	handler := newTestEthScanLogsHandler(db)

	hCalls, boundaryCalls := 0, 0
	client := &fakeEthScanClient{blockFn: func(number int64) (*web3types.Block, error) {
		switch number {
		case 1:
			hCalls++
			return &web3types.Block{Number: big.NewInt(number), Hash: common.HexToHash("0x11")}, nil
		case 0:
			boundaryCalls++
			hash := common.HexToHash("0x12")
			if boundaryCalls == 2 {
				hash = dbHash
			}
			return &web3types.Block{Number: big.NewInt(number), Hash: hash}, nil
		default:
			t.Fatalf("unexpected block lookup %d", number)
			return nil, nil
		}
	}}

	result, err := handler.ScanLogs(
		context.Background(),
		client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 1},
			BlockRange:        citypes.RangeUint64{From: 1, To: 1},
		},
		&EthPivotAssumption{BlockNumber: 0, BlockHash: dbHash},
	)

	require.NoError(t, err)
	assert.Empty(t, result.Logs)
	assert.Equal(t, 4, hCalls, "each FN attempt must read its checkpoint before and after")
	assert.Equal(t, 2, boundaryCalls)
	assert.Len(t, client.filters, 2, "the boundary mismatch must replay exactly one FN attempt")
}

func TestEthScanLogsRepeatedBoundaryMismatchRejectsCandidate(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	dbHash := common.HexToHash("0x10")
	insertScanLogsMapping(t, db, 0, 0, 0, dbHash.String())
	insertEthScanLogsTestPartition(t, db, 0)
	handler := newTestEthScanLogsHandler(db)
	logReads := 0
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(
		"scanlogs_test_count_boundary_log_reads", func(tx *gorm.DB) {
			if tx.Statement.Table == "logs_0" {
				logReads++
			}
		},
	))

	hCalls, boundaryCalls := 0, 0
	client := &fakeEthScanClient{blockFn: func(number int64) (*web3types.Block, error) {
		switch number {
		case 1:
			hCalls++
			return &web3types.Block{Number: big.NewInt(number), Hash: common.HexToHash("0x11")}, nil
		case 0:
			boundaryCalls++
			return &web3types.Block{Number: big.NewInt(number), Hash: common.HexToHash("0x12")}, nil
		default:
			t.Fatalf("unexpected block lookup %d", number)
			return nil, nil
		}
	}}

	result, err := handler.ScanLogs(
		context.Background(),
		client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 2},
			BlockRange:        citypes.RangeUint64{From: 0, To: 1},
		},
		&EthPivotAssumption{BlockNumber: 0, BlockHash: dbHash},
	)

	assert.Nil(t, result)
	require.ErrorIs(t, err, ErrScanLogsConsistency)
	assert.Equal(t, 4, hCalls)
	assert.Equal(t, 2, boundaryCalls)
	assert.Len(t, client.filters, 2, "only one boundary retry is allowed")
	assert.Equal(t, 1, logReads, "boundary-only inner retry must reuse the DB cache")
}

func TestEthScanLogsRejectsNullFNAssumptionBlock(t *testing.T) {
	handler := newTestEthScanLogsHandler(newScanLogsHandlerTestDB(t))
	assumptionHash := common.HexToHash("0x12")
	client := &fakeEthScanClient{}
	client.blockFn = func(number int64) (*web3types.Block, error) {
		require.Equal(t, int64(12), number)
		switch client.blockCalls {
		case 1, 3:
			return &web3types.Block{Number: big.NewInt(number), Hash: assumptionHash}, nil
		case 2:
			return nil, nil
		default:
			t.Fatalf("unexpected block lookup call %d", client.blockCalls)
			return nil, nil
		}
	}

	_, err := handler.ScanLogs(
		context.Background(),
		client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 1},
			BlockRange:        citypes.RangeUint64{From: 10, To: 10},
		},
		&EthPivotAssumption{BlockNumber: 12, BlockHash: assumptionHash},
	)

	require.ErrorIs(t, err, ErrScanLogsAssumptionFailure)
	assert.Equal(t, 3, client.blockCalls)
	assert.Len(t, client.filters, 1)
}

func TestEthScanLogsRejectsNullCheckpointForFNAssumption(t *testing.T) {
	handler := newTestEthScanLogsHandler(newScanLogsHandlerTestDB(t))
	client := &fakeEthScanClient{}

	_, err := handler.ScanLogs(
		context.Background(),
		client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 1},
			BlockRange:        citypes.RangeUint64{From: 10, To: 10},
		},
		&EthPivotAssumption{BlockNumber: 12, BlockHash: common.HexToHash("0x12")},
	)

	require.ErrorIs(t, err, ErrScanLogsAssumptionFailure)
	assert.Equal(t, 1, client.blockCalls)
	assert.Empty(t, client.filters)
}

func TestEthScanLogsBoundaryBackoffPreservesContextError(t *testing.T) {
	db := newScanLogsHandlerTestDB(t)
	dbHash := common.HexToHash("0x10")
	insertScanLogsMapping(t, db, 0, 0, 0, dbHash.String())
	handler := newTestEthScanLogsHandler(db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hCalls := 0
	client := &fakeEthScanClient{blockFn: func(number int64) (*web3types.Block, error) {
		switch number {
		case 1:
			hCalls++
			if hCalls == 2 {
				cancel()
			}
			return &web3types.Block{Number: big.NewInt(1), Hash: common.HexToHash("0x11")}, nil
		case 0:
			return &web3types.Block{Number: big.NewInt(0), Hash: common.HexToHash("0x12")}, nil
		default:
			t.Fatalf("unexpected block lookup %d", number)
			return nil, nil
		}
	}}

	_, err := handler.ScanLogs(
		ctx,
		client,
		EthScanLogParams{
			EthScanLogRequest: &EthScanLogRequest{Limit: 1},
			BlockRange:        citypes.RangeUint64{From: 1, To: 1},
		},
		&EthPivotAssumption{BlockNumber: 0, BlockHash: dbHash},
	)

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 2, hCalls)
	assert.Len(t, client.filters, 1)
}

func TestEthFNReaderDoesNotApplyCursorIndexToLaterBlocks(t *testing.T) {
	client := &fakeEthScanClient{logs: []web3types.Log{{
		BlockNumber: 5010, BlockHash: common.HexToHash("0x5010"), Index: 0,
	}}}
	reader := ethFNReader{
		client: client,
		spec: ethFNReaderSpec{
			blocks: scanRange{From: 5000, To: 5015}, cursor: &store.ScanCursor{BlockNumber: 5009, LogIndex: 2},
			windowSize: 100,
		},
	}
	batch, err := reader.Scan(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, batch.Logs, 1)
	assert.Equal(t, uint64(5010), batch.Logs[0].BlockNumber)
	assert.Equal(t, uint(0), batch.Logs[0].Index)
	require.Len(t, client.filters, 1)
	assert.Equal(t, web3types.BlockNumber(5009), *client.filters[0].FromBlock)
}

func TestEthFNReaderClassifiesCursorRangeReason(t *testing.T) {
	client := &fakeEthScanClient{}
	reader := ethFNReader{
		client: client,
		spec: ethFNReaderSpec{
			blocks: scanRange{From: 5000, To: 5015},
			cursor: &store.ScanCursor{BlockNumber: 4999},
		},
	}

	_, err := reader.Scan(context.Background(), 10)
	require.ErrorIs(t, err, ErrScanLogsInvalidCursor)
	require.EqualError(t, err, "invalid scan logs cursor: cursor 4999 is outside block range [5000, 5015]")
	assert.Empty(t, client.filters)
}

func TestEthFNReaderUsesHigherFenceInsteadOfCursorHashLookup(t *testing.T) {
	// Reader knows only cursor BN/logIndex. The hash below is accepted as the
	// canonical hash returned by getLogs; ScanLogs' higher H before/after fence
	// protects that ancestry without a separate BlockByNumber(5009) RPC.
	canonicalHash := common.HexToHash("0xaaa5009")
	reader := ethFNReader{
		client: &fakeEthScanClient{logs: []web3types.Log{
			{BlockNumber: 5009, BlockHash: canonicalHash, Index: 1},
			{BlockNumber: 5009, BlockHash: canonicalHash, Index: 3},
		}},
		spec: ethFNReaderSpec{
			blocks:     scanRange{From: 5009, To: 5015},
			cursor:     &store.ScanCursor{BlockNumber: 5009, LogIndex: 2},
			windowSize: 100,
		},
	}
	batch, err := reader.Scan(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, batch.Logs, 1)
	assert.Equal(t, uint(3), batch.Logs[0].Index)
}

func TestEthFNReaderReverseReturnsResponseDirectionTail(t *testing.T) {
	logs := []web3types.Log{
		{BlockNumber: 5009, BlockHash: common.HexToHash("0x5009"), Index: 1},
		{BlockNumber: 5010, BlockHash: common.HexToHash("0x5010"), Index: 0},
	}
	reader := ethFNReader{
		client: &fakeEthScanClient{logs: logs},
		spec: ethFNReaderSpec{
			blocks: scanRange{From: 5009, To: 5010}, reverse: true, windowSize: 100,
		},
	}
	batch, err := reader.Scan(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, batch.Logs, 2)
	assert.Equal(t, uint64(5010), batch.Logs[0].BlockNumber)
	assert.Equal(t, uint64(5009), batch.Logs[1].BlockNumber)
	assert.Equal(t, &store.ScanCursor{BlockNumber: 5009, LogIndex: 1}, batch.TailPosition)
}

func TestEthPivotGuardDirectionRules(t *testing.T) {
	assumption := &EthPivotAssumption{BlockNumber: 6000, BlockHash: common.HexToHash("0x6000")}
	logs := []web3types.Log{
		{BlockNumber: 5001, BlockHash: common.HexToHash("0x5001")},
		{BlockNumber: 5002, BlockHash: common.HexToHash("0x5002")},
	}
	forward := finishEthCandidate(EthScanLogParams{
		EthScanLogRequest: &EthScanLogRequest{},
	}, assumption, logs, nil, ethScanUsage{fn: true})
	assert.Equal(t, hexutil.Uint64(5002), forward.PivotGuard.BlockNumber)

	reverseFirst := finishEthCandidate(EthScanLogParams{
		EthScanLogRequest: &EthScanLogRequest{Reverse: true},
	}, assumption, logs, nil, ethScanUsage{fn: true})
	assert.Equal(t, hexutil.Uint64(5001), reverseFirst.PivotGuard.BlockNumber)

	reverseContinuation := finishEthCandidate(EthScanLogParams{
		EthScanLogRequest: &EthScanLogRequest{
			Reverse: true, Cursor: &ScanLogCursor{BlockNumber: 5000},
		},
	}, assumption, logs, nil, ethScanUsage{fn: true})
	assert.Equal(t, EthPivotGuard(*assumption), *reverseContinuation.PivotGuard)
}

func TestFinishCfxCandidateRejectsMissingGuardEpoch(t *testing.T) {
	valid := cfxLog("0x10", 10, 0)
	missingEpoch := cfxLog("0x11", 11, 0)
	missingEpoch.EpochNumber = nil
	assumption := &CfxPivotAssumption{EpochNumber: 10, PivotBlockHash: "0x10"}

	tests := []struct {
		name    string
		reverse bool
		logs    []cfxtypes.Log
	}{
		{name: "forward tail", logs: []cfxtypes.Log{valid, missingEpoch}},
		{name: "reverse head", reverse: true, logs: []cfxtypes.Log{missingEpoch, valid}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := (&CfxLogsApiHandler{}).finishCfxCandidate(
				CfxScanLogParams{CfxScanLogRequest: &CfxScanLogRequest{Reverse: test.reverse}},
				assumption,
				cfxScanGeneration{},
				test.logs,
				nil,
				false,
				false,
				nil,
			)

			require.EqualError(t, err, "incomplete guard log: missing epoch number")
		})
	}
}

func cfxSummary(hash cfxtypes.Hash, epoch, block uint64) *cfxtypes.BlockSummary {
	return &cfxtypes.BlockSummary{BlockHeader: cfxtypes.BlockHeader{
		Hash: hash, EpochNumber: testHexBig(epoch), BlockNumber: testHexBig(block),
	}}
}

func cfxLog(hash cfxtypes.Hash, epoch, index uint64) cfxtypes.Log {
	return cfxtypes.Log{BlockHash: &hash, EpochNumber: testHexBig(epoch), LogIndex: testHexBig(index)}
}

func testHexBig(value uint64) *hexutil.Big {
	return (*hexutil.Big)(new(big.Int).SetUint64(value))
}

func TestWhitelistedFNOversizedErrorUsesMessageOnly(t *testing.T) {
	assert.True(t, isWhitelistedFNOversizedError(fmt.Errorf("the query set is too large")))
	assert.True(t, isWhitelistedFNOversizedError(fmt.Errorf(" This Query Results In Too Many Logs ")))
	assert.False(t, isWhitelistedFNOversizedError(fmt.Errorf("execution reverted")))
	assert.False(t, isWhitelistedFNOversizedError(fmt.Errorf(
		"query returned more than 10000 results. Try with this block range: [0x10, 0x20]",
	)))
}

func TestScanFNBlockWindowsShrinksOversizedRangesToSuccess(t *testing.T) {
	tests := []struct {
		name      string
		reverse   bool
		wantLogs  []uint64
		wantCalls [][2]uint64
	}{
		{
			name:     "forward",
			wantLogs: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9},
			wantCalls: [][2]uint64{
				{1, 8}, {1, 4}, {1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 9},
			},
		},
		{
			name:     "reverse",
			reverse:  true,
			wantLogs: []uint64{9, 8, 7, 6, 5, 4, 3, 2, 1},
			wantCalls: [][2]uint64{
				{2, 9}, {6, 9}, {8, 9}, {6, 7}, {4, 5}, {2, 3}, {1, 1},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls [][2]uint64
			logs, err := scanFNBlockWindows(
				context.Background(), scanRange{From: 1, To: 9}, test.reverse, 8, 9,
				func(_ context.Context, low, high uint64) ([]uint64, error) {
					calls = append(calls, [2]uint64{low, high})
					if high-low+1 > 2 {
						return nil, errors.New("the query set is too large")
					}
					window := make([]uint64, 0, high-low+1)
					for block := low; block <= high; block++ {
						window = append(window, block)
					}
					return window, nil
				},
				nil,
			)

			require.NoError(t, err)
			assert.Equal(t, test.wantLogs, logs)
			assert.Equal(t, test.wantCalls, calls)
		})
	}
}

func TestScanFNBlockWindowsStopsOnSingleHeightOversizedError(t *testing.T) {
	wantErr := errors.New("the query set is too large")
	calls := 0
	logs, err := scanFNBlockWindows(
		context.Background(), scanRange{From: 7, To: 7}, false, 100, 1,
		func(_ context.Context, low, high uint64) ([]int, error) {
			calls++
			assert.Equal(t, uint64(7), low)
			assert.Equal(t, uint64(7), high)
			return nil, wantErr
		},
		nil,
	)

	assert.Nil(t, logs)
	assert.ErrorIs(t, err, wantErr)
	assert.Equal(t, 1, calls)
}

func TestScanFNBlockWindowsReturnsNonWhitelistedErrorImmediately(t *testing.T) {
	wantErr := errors.New("execution reverted")
	calls := 0
	logs, err := scanFNBlockWindows(
		context.Background(), scanRange{From: 1, To: 100}, false, 64, 10,
		func(context.Context, uint64, uint64) ([]int, error) {
			calls++
			return nil, wantErr
		},
		nil,
	)

	assert.Nil(t, logs)
	assert.ErrorIs(t, err, wantErr)
	assert.Equal(t, 1, calls)
}
