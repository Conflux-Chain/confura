package mysql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type scanPartitionCall struct {
	index     uint32
	blockFrom uint64
	blockTo   uint64
	remaining int
}

func TestEffectiveScanLogBlockRange(t *testing.T) {
	tests := []struct {
		name   string
		params store.ScanLogParams
		want   types.RangeUint64
	}{
		{
			name: "without cursor",
			params: store.ScanLogParams{
				Filter: store.ScanLogFilter{BlockFrom: 100, BlockTo: 200},
			},
			want: types.RangeUint64{From: 100, To: 200},
		},
		{
			name: "forward cursor raises lower bound",
			params: store.ScanLogParams{
				Filter: store.ScanLogFilter{BlockFrom: 100, BlockTo: 200},
				Cursor: &store.ScanCursor{BlockNumber: 150},
			},
			want: types.RangeUint64{From: 150, To: 200},
		},
		{
			name: "reverse cursor lowers upper bound",
			params: store.ScanLogParams{
				Filter:  store.ScanLogFilter{BlockFrom: 100, BlockTo: 200},
				Cursor:  &store.ScanCursor{BlockNumber: 150},
				Reverse: true,
			},
			want: types.RangeUint64{From: 100, To: 150},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, effectiveScanLogBlockRange(test.params))
		})
	}
}

func TestScanPartitionsForwardStopsAtLimit(t *testing.T) {
	partitions := testScanPartitions()
	params := store.ScanLogParams{
		Filter: store.ScanLogFilter{BlockFrom: 105, BlockTo: 125},
		Limit:  2,
	}

	var calls []scanPartitionCall
	logs, err := scanPartitions(
		context.Background(), partitions, params,
		func(
			_ context.Context, partition *bnPartition,
			blockFrom, blockTo uint64, remaining int,
		) ([]*store.Log, error) {
			calls = append(calls, scanPartitionCall{
				index: partition.Index, blockFrom: blockFrom,
				blockTo: blockTo, remaining: remaining,
			})
			return []*store.Log{{BlockNumber: blockFrom, LogIndex: uint64(partition.Index)}}, nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, []scanPartitionCall{
		{index: 0, blockFrom: 105, blockTo: 109, remaining: 2},
		{index: 1, blockFrom: 110, blockTo: 119, remaining: 1},
	}, calls)
	assert.Equal(t, [][2]uint64{{105, 0}, {110, 1}}, storeLogKeys(logs))
}

func TestScanPartitionsForwardPrunesAndTightensToCursor(t *testing.T) {
	params := store.ScanLogParams{
		Filter: store.ScanLogFilter{BlockFrom: 100, BlockTo: 129},
		Cursor: &store.ScanCursor{BlockNumber: 115, LogIndex: 3},
		Limit:  3,
	}

	var calls []scanPartitionCall
	logs, err := scanPartitions(
		context.Background(), testScanPartitions(), params,
		func(
			_ context.Context, partition *bnPartition,
			blockFrom, blockTo uint64, remaining int,
		) ([]*store.Log, error) {
			calls = append(calls, scanPartitionCall{
				index: partition.Index, blockFrom: blockFrom,
				blockTo: blockTo, remaining: remaining,
			})
			return []*store.Log{{BlockNumber: blockFrom, LogIndex: uint64(partition.Index)}}, nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, []scanPartitionCall{
		{index: 1, blockFrom: 115, blockTo: 119, remaining: 3},
		{index: 2, blockFrom: 120, blockTo: 129, remaining: 2},
	}, calls)
	assert.Equal(t, [][2]uint64{{115, 1}, {120, 2}}, storeLogKeys(logs))
}

func TestScanPartitionsReversePrunesAndTightensToCursor(t *testing.T) {
	partitions := testScanPartitions()
	params := store.ScanLogParams{
		Filter:  store.ScanLogFilter{BlockFrom: 100, BlockTo: 129},
		Cursor:  &store.ScanCursor{BlockNumber: 115, LogIndex: 3},
		Reverse: true,
		Limit:   3,
	}

	var calls []scanPartitionCall
	logs, err := scanPartitions(
		context.Background(), partitions, params,
		func(
			_ context.Context, partition *bnPartition,
			blockFrom, blockTo uint64, remaining int,
		) ([]*store.Log, error) {
			calls = append(calls, scanPartitionCall{
				index: partition.Index, blockFrom: blockFrom,
				blockTo: blockTo, remaining: remaining,
			})
			return []*store.Log{{BlockNumber: blockTo, LogIndex: uint64(partition.Index)}}, nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, []scanPartitionCall{
		{index: 1, blockFrom: 110, blockTo: 115, remaining: 3},
		{index: 0, blockFrom: 100, blockTo: 109, remaining: 2},
	}, calls)
	assert.Equal(t, [][2]uint64{{115, 1}, {109, 0}}, storeLogKeys(logs))
}

func TestScanPartitionsRejectsQueryLimitViolation(t *testing.T) {
	params := store.ScanLogParams{
		Filter: store.ScanLogFilter{BlockFrom: 100, BlockTo: 109},
		Limit:  1,
	}

	logs, err := scanPartitions(
		context.Background(), testScanPartitions()[:1], params,
		func(
			context.Context, *bnPartition, uint64, uint64, int,
		) ([]*store.Log, error) {
			return []*store.Log{{}, {}}, nil
		},
	)
	assert.Nil(t, logs)
	assert.ErrorContains(t, err, "remaining limit")
}

func TestScanPartitionsStopsWhenContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	logs, err := scanPartitions(
		ctx,
		testScanPartitions(),
		store.ScanLogParams{
			Filter: store.ScanLogFilter{BlockFrom: 100, BlockTo: 129},
			Limit:  1,
		},
		func(
			context.Context, *bnPartition, uint64, uint64, int,
		) ([]*store.Log, error) {
			called = true
			return nil, nil
		},
	)
	assert.Nil(t, logs)
	assert.ErrorIs(t, err, store.ErrGetLogsTimeout)
	assert.False(t, called)
}

func TestValidateScanLogParams(t *testing.T) {
	tests := []struct {
		name   string
		params store.ScanLogParams
		valid  bool
	}{
		{
			name: "valid",
			params: store.ScanLogParams{
				Filter: store.ScanLogFilter{BlockFrom: 10, BlockTo: 10},
				Limit:  1,
			},
			valid: true,
		},
		{
			name: "cursor at lower bound",
			params: store.ScanLogParams{
				Filter: store.ScanLogFilter{BlockFrom: 10, BlockTo: 20},
				Cursor: &store.ScanCursor{BlockNumber: 10},
				Limit:  1,
			},
			valid: true,
		},
		{
			name: "cursor at upper bound",
			params: store.ScanLogParams{
				Filter: store.ScanLogFilter{BlockFrom: 10, BlockTo: 20},
				Cursor: &store.ScanCursor{BlockNumber: 20},
				Limit:  1,
			},
			valid: true,
		},
		{
			name: "maximum limit",
			params: store.ScanLogParams{
				Filter: store.ScanLogFilter{BlockFrom: 10, BlockTo: 10},
				Limit:  int(store.MaxLogLimit),
			},
			valid: true,
		},
		{name: "zero limit", params: store.ScanLogParams{}},
		{
			name: "negative limit",
			params: store.ScanLogParams{
				Filter: store.ScanLogFilter{BlockFrom: 10, BlockTo: 10},
				Limit:  -1,
			},
		},
		{
			name: "limit exceeds maximum",
			params: store.ScanLogParams{
				Filter: store.ScanLogFilter{BlockFrom: 10, BlockTo: 10},
				Limit:  int(store.MaxLogLimit) + 1,
			},
		},
		{
			name: "inverted range",
			params: store.ScanLogParams{
				Filter: store.ScanLogFilter{BlockFrom: 11, BlockTo: 10},
				Limit:  1,
			},
		},
		{
			name: "cursor before range",
			params: store.ScanLogParams{
				Filter: store.ScanLogFilter{BlockFrom: 10, BlockTo: 20},
				Cursor: &store.ScanCursor{BlockNumber: 9},
				Limit:  1,
			},
		},
		{
			name: "cursor after range",
			params: store.ScanLogParams{
				Filter: store.ScanLogFilter{BlockFrom: 10, BlockTo: 20},
				Cursor: &store.ScanCursor{BlockNumber: 21},
				Limit:  1,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateScanLogParams(test.params)
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestMysqlStoreScanLogsRoutesSupportedFilters(t *testing.T) {
	t.Run("universal logs", func(t *testing.T) {
		ms, db := newScanMysqlStoreTestHarness(t)
		require.NoError(t, db.Exec(`CREATE TABLE logs_0 (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			bn INTEGER NOT NULL,
			epoch INTEGER NOT NULL,
			topic0 TEXT NOT NULL,
			topic1 TEXT, topic2 TEXT, topic3 TEXT,
			log_index INTEGER NOT NULL,
			extra BLOB
		)`).Error)
		require.NoError(t, db.Create(&bnPartition{
			Entity: bnPartitionedLogEntity,
			Index:  0,
			Count:  2,
			BnMin:  sql.NullInt64{Int64: 10, Valid: true},
			BnMax:  sql.NullInt64{Int64: 11, Valid: true},
		}).Error)
		require.NoError(t, db.Table("logs_0").Create([]*log{
			{BlockNumber: 10, Epoch: 10, Topic0: "topic", LogIndex: 0},
			{BlockNumber: 11, Epoch: 11, Topic0: "topic", LogIndex: 0},
		}).Error)

		logs, err := ms.ScanLogs(context.Background(), store.ScanLogParams{
			Filter: store.ScanLogFilter{BlockFrom: 10, BlockTo: 11},
			Limit:  1,
		})
		require.NoError(t, err)
		assert.Equal(t, [][2]uint64{{10, 0}}, storeLogKeys(logs))

		_, err = ms.ScanLogs(context.Background(), store.ScanLogParams{
			Filter: store.ScanLogFilter{BlockFrom: 9, BlockTo: 11},
			Limit:  1,
		})
		assert.ErrorIs(t, err, store.ErrAlreadyPruned)

		logs, err = ms.ScanLogs(context.Background(), store.ScanLogParams{
			Filter: store.ScanLogFilter{BlockFrom: 9, BlockTo: 11},
			Cursor: &store.ScanCursor{BlockNumber: 10, LogIndex: 0},
			Limit:  10,
		})
		require.NoError(t, err)
		assert.Equal(t, [][2]uint64{{11, 0}}, storeLogKeys(logs))
	})

	t.Run("address and topic use the address hash table", func(t *testing.T) {
		ms, db := newScanMysqlStoreTestHarness(t)
		createAddressScanTable(t, db)

		cid, _, err := ms.cs.AddContractIfAbsent("contract")
		require.NoError(t, err)
		tid, _, err := ms.ts.GetOrCreate("topic")
		require.NoError(t, err)
		require.NoError(t, db.Table(ms.ails.GetPartitionedTableName("contract")).Create(
			&AddressIndexedLog{
				ContractID: cid, Topic0ID: tid,
				BlockNumber: 10, Epoch: 10, LogIndex: 2,
			},
		).Error)

		logs, err := ms.ScanLogs(context.Background(), store.ScanLogParams{
			Filter: store.ScanLogFilter{
				BlockFrom: 0, BlockTo: 20,
				Contract: "contract", Topic0: "topic",
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, logs, 1)
		assert.Equal(t, "topic", logs[0].Topic0)
		assert.Equal(t, uint64(2), logs[0].LogIndex)
	})

	t.Run("topic uses the topic hash table", func(t *testing.T) {
		ms, db := newScanMysqlStoreTestHarness(t)
		createTopicScanTable(t, db)

		tid, _, err := ms.ts.GetOrCreate("topic")
		require.NoError(t, err)
		require.NoError(t, db.Table(ms.tils.GetPartitionedTableName("topic")).Create(
			&TopicIndexedLog{
				Topic0ID: tid, BlockNumber: 10, Epoch: 10, LogIndex: 3,
			},
		).Error)

		logs, err := ms.ScanLogs(context.Background(), store.ScanLogParams{
			Filter: store.ScanLogFilter{
				BlockFrom: 0, BlockTo: 20, Topic0: "topic",
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, logs, 1)
		assert.Equal(t, "topic", logs[0].Topic0)
		assert.Equal(t, uint64(3), logs[0].LogIndex)
	})

	t.Run("migrated contract uses dedicated partitions", func(t *testing.T) {
		ms, db := newScanMysqlStoreTestHarness(t)
		cid, _, err := ms.cs.AddContractIfAbsent("contract")
		require.NoError(t, err)
		tid, _, err := ms.ts.GetOrCreate("topic")
		require.NoError(t, err)

		tableName := ms.bcls.getPartitionedTableName(ms.bcls.contractTabler(cid), 0)
		createDedicatedContractScanTable(t, db, tableName)
		require.NoError(t, db.Create(&bnPartition{
			Entity: ms.bcls.contractEntity(cid), Index: 0, Count: 1,
			BnMin: sql.NullInt64{Int64: 10, Valid: true},
			BnMax: sql.NullInt64{Int64: 10, Valid: true},
		}).Error)
		require.NoError(t, db.Table(tableName).Create(&contractLog{
			BlockNumber: 10, Epoch: 10, Topic0ID: tid, LogIndex: 4,
		}).Error)

		logs, err := ms.ScanLogs(context.Background(), store.ScanLogParams{
			Filter: store.ScanLogFilter{
				BlockFrom: 10, BlockTo: 10,
				Contract: "contract", Topic0: "topic",
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, logs, 1)
		assert.Equal(t, "topic", logs[0].Topic0)
		assert.Equal(t, uint64(4), logs[0].LogIndex)

		logs, err = ms.ScanLogs(context.Background(), store.ScanLogParams{
			Filter: store.ScanLogFilter{
				BlockFrom: 0, BlockTo: 10,
				Contract: "contract", Topic0: "topic",
			},
			Cursor: &store.ScanCursor{BlockNumber: 10, LogIndex: 3},
			Limit:  10,
		})
		require.NoError(t, err)
		require.Len(t, logs, 1)
		assert.Equal(t, uint64(4), logs[0].LogIndex)
	})

	t.Run("migrated topic uses dedicated partitions", func(t *testing.T) {
		ms, db := newScanMysqlStoreTestHarness(t)
		tid, _, err := ms.ts.GetOrCreate("topic")
		require.NoError(t, err)

		tableName := ms.btls.getPartitionedTableName(ms.btls.topicTabler(tid), 0)
		createDedicatedTopicScanTable(t, db, tableName)
		require.NoError(t, db.Create(&bnPartition{
			Entity: ms.btls.topicEntity(tid), Index: 0, Count: 1,
			BnMin: sql.NullInt64{Int64: 10, Valid: true},
			BnMax: sql.NullInt64{Int64: 10, Valid: true},
		}).Error)
		require.NoError(t, db.Table(tableName).Create(&topicLog{
			BlockNumber: 10, Epoch: 10, LogIndex: 5,
		}).Error)

		logs, err := ms.ScanLogs(context.Background(), store.ScanLogParams{
			Filter: store.ScanLogFilter{
				BlockFrom: 10, BlockTo: 10, Topic0: "topic",
			},
			Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, logs, 1)
		assert.Equal(t, "topic", logs[0].Topic0)
		assert.Equal(t, uint64(5), logs[0].LogIndex)

		logs, err = ms.ScanLogs(context.Background(), store.ScanLogParams{
			Filter: store.ScanLogFilter{
				BlockFrom: 0, BlockTo: 10, Topic0: "topic",
			},
			Cursor: &store.ScanCursor{BlockNumber: 10, LogIndex: 4},
			Limit:  10,
		})
		require.NoError(t, err)
		require.Len(t, logs, 1)
		assert.Equal(t, uint64(5), logs[0].LogIndex)
	})
}

func testScanPartitions() []*bnPartition {
	return []*bnPartition{
		{
			Index: 0,
			BnMin: sql.NullInt64{Int64: 100, Valid: true},
			BnMax: sql.NullInt64{Int64: 109, Valid: true},
		},
		{
			Index: 1,
			BnMin: sql.NullInt64{Int64: 110, Valid: true},
			BnMax: sql.NullInt64{Int64: 119, Valid: true},
		},
		{
			Index: 2,
			BnMin: sql.NullInt64{Int64: 120, Valid: true},
			BnMax: sql.NullInt64{Int64: 129, Valid: true},
		},
	}
}

func storeLogKeys(logs []*store.Log) [][2]uint64 {
	keys := make([][2]uint64, 0, len(logs))
	for _, log := range logs {
		keys = append(keys, [2]uint64{log.BlockNumber, log.LogIndex})
	}
	return keys
}

func newScanMysqlStoreTestHarness(t *testing.T) (*MysqlStore[*store.EpochData], *gorm.DB) {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&bnPartition{}))
	require.NoError(t, db.Exec(`CREATE TABLE contracts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		address TEXT NOT NULL UNIQUE,
		log_count INTEGER NOT NULL DEFAULT 0,
		latest_updated_epoch INTEGER NOT NULL DEFAULT 0
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE topics (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		hash TEXT NOT NULL UNIQUE,
		log_count INTEGER NOT NULL DEFAULT 0,
		latest_updated_epoch INTEGER NOT NULL DEFAULT 0
	)`).Error)

	cs := NewContractStore(db)
	ts := NewTopicStore(db)
	ails := NewAddressIndexedLogStore[*store.EpochData](db, cs, ts, 1)
	tils := NewTopicIndexedLogStore[*store.EpochData](db, ts, 1)
	ls := newLogStore[*store.EpochData](db, cs, nil, nil)
	bcls := newBigContractLogStore[*store.EpochData](db, cs, ts, nil, ails, nil)
	btls := newBigTopicLogStore[*store.EpochData](db, ts, nil, tils, nil)

	return &MysqlStore[*store.EpochData]{
		ls: ls, ails: ails, bcls: bcls, cs: cs,
		tils: tils, btls: btls, ts: ts,
	}, db
}

func createAddressScanTable(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(`CREATE TABLE addr_logs_0 (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		cid INTEGER NOT NULL,
		bn INTEGER NOT NULL,
		epoch INTEGER NOT NULL,
		tid INTEGER NOT NULL,
		topic1 TEXT, topic2 TEXT, topic3 TEXT,
		log_index INTEGER NOT NULL,
		extra BLOB
	)`).Error)
}

func createTopicScanTable(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(`CREATE TABLE topic_logs_0 (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		bn INTEGER NOT NULL,
		epoch INTEGER NOT NULL,
		tid INTEGER NOT NULL,
		topic1 TEXT, topic2 TEXT, topic3 TEXT,
		log_index INTEGER NOT NULL,
		extra BLOB
	)`).Error)
}

func createDedicatedContractScanTable(t *testing.T, db *gorm.DB, tableName string) {
	t.Helper()
	require.NoError(t, db.Exec(`CREATE TABLE `+tableName+` (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		bn INTEGER NOT NULL,
		epoch INTEGER NOT NULL,
		tid INTEGER NOT NULL,
		topic1 TEXT, topic2 TEXT, topic3 TEXT,
		log_index INTEGER NOT NULL,
		extra BLOB
	)`).Error)
}

func createDedicatedTopicScanTable(t *testing.T, db *gorm.DB, tableName string) {
	t.Helper()
	require.NoError(t, db.Exec(`CREATE TABLE `+tableName+` (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		bn INTEGER NOT NULL,
		epoch INTEGER NOT NULL,
		topic1 TEXT, topic2 TEXT, topic3 TEXT,
		log_index INTEGER NOT NULL,
		extra BLOB
	)`).Error)
}
