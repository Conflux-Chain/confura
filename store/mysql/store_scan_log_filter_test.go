package mysql

import (
	"context"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestScanLogFilterKeysetOrderingAndLimit(t *testing.T) {
	db := newScanLogFilterTestDB(t)
	tableName := "scan_logs"
	require.NoError(t, db.Table(tableName).AutoMigrate(&log{}))
	require.NoError(t, db.Table(tableName).Create([]*log{
		{BlockNumber: 10, LogIndex: 1, Topic0: "a"},
		{BlockNumber: 10, LogIndex: 2, Topic0: "a"},
		{BlockNumber: 11, LogIndex: 0, Topic0: "b"},
		{BlockNumber: 12, LogIndex: 0, Topic0: "a"},
	}).Error)

	tests := []struct {
		name    string
		cursor  *store.ScanCursor
		reverse bool
		limit   int
		want    [][2]uint64
	}{
		{
			name:   "forward cursor is exclusive within the same block",
			cursor: &store.ScanCursor{BlockNumber: 10, LogIndex: 1},
			limit:  2,
			want:   [][2]uint64{{10, 2}, {11, 0}},
		},
		{
			name:    "reverse cursor is exclusive within the same block",
			cursor:  &store.ScanCursor{BlockNumber: 11, LogIndex: 0},
			reverse: true,
			limit:   2,
			want:    [][2]uint64{{10, 2}, {10, 1}},
		},
		{
			name:    "reverse first page",
			reverse: true,
			limit:   3,
			want:    [][2]uint64{{12, 0}, {11, 0}, {10, 2}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filter := scanLogFilter{TableName: tableName, BlockFrom: 10, BlockTo: 12}
			var rows []*log
			err := filter.find(
				context.Background(), db, test.cursor, test.reverse, test.limit, &rows,
			)
			require.NoError(t, err)
			assert.Equal(t, test.want, scanLogKeys(rows))
		})
	}
}

func TestAddressIndexedScanLogFilterPredicates(t *testing.T) {
	db := newScanLogFilterTestDB(t)
	tableName := "scan_address_logs"
	require.NoError(t, db.Table(tableName).AutoMigrate(&AddressIndexedLog{}))
	require.NoError(t, db.Table(tableName).Create([]*AddressIndexedLog{
		{ContractID: 1, Topic0ID: 7, BlockNumber: 10, LogIndex: 0},
		{ContractID: 1, Topic0ID: 8, BlockNumber: 11, LogIndex: 0},
		{ContractID: 2, Topic0ID: 7, BlockNumber: 12, LogIndex: 0},
	}).Error)

	topicID := uint64(7)
	filter := AddressIndexedScanLogFilter{
		scanLogFilter: scanLogFilter{
			TableName: tableName,
			BlockFrom: 10,
			BlockTo:   12,
			Topic0ID:  &topicID,
		},
		ContractID: 1,
	}

	rows, err := filter.Find(context.Background(), db, nil, false, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, uint64(1), rows[0].ContractID)
	assert.Equal(t, uint64(7), rows[0].Topic0ID)
}

func TestTopicIndexedScanLogFilterPredicate(t *testing.T) {
	db := newScanLogFilterTestDB(t)
	tableName := "scan_topic_logs"
	require.NoError(t, db.Table(tableName).AutoMigrate(&TopicIndexedLog{}))
	require.NoError(t, db.Table(tableName).Create([]*TopicIndexedLog{
		{Topic0ID: 7, BlockNumber: 10, LogIndex: 0},
		{Topic0ID: 8, BlockNumber: 11, LogIndex: 0},
	}).Error)

	filter := TopicIndexedScanLogFilter{
		scanLogFilter: scanLogFilter{
			TableName: tableName,
			BlockFrom: 10,
			BlockTo:   11,
		},
		TopicID: 8,
	}

	rows, err := filter.Find(context.Background(), db, nil, false, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, uint64(8), rows[0].Topic0ID)
}

func TestIndexedScanLogFiltersForceRouteIndex(t *testing.T) {
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN:                       "gorm:gorm@tcp(127.0.0.1:9910)/gorm?parseTime=True",
		SkipInitializeWithVersion: true,
	}), &gorm.Config{DryRun: true, DisableAutomaticPing: true})
	require.NoError(t, err)

	tests := []struct {
		name      string
		find      func(*gorm.DB) error
		forceHint string
	}{
		{
			name: "universal or dedicated without topic0",
			find: func(db *gorm.DB) error {
				filter := scanLogFilter{TableName: "logs_0", BlockTo: 100}
				var rows []*log
				return filter.find(context.Background(), db, nil, false, 10, &rows)
			},
			forceHint: "FORCE INDEX (`idx_bn_li`)",
		},
		{
			name: "dedicated contract with topic0",
			find: func(db *gorm.DB) error {
				topicID := uint64(2)
				filter := scanLogFilter{
					TableName: "clogs_1_0", BlockTo: 100, Topic0ID: &topicID,
				}
				var rows []*log
				return filter.find(context.Background(), db, nil, false, 10, &rows)
			},
			forceHint: "FORCE INDEX (`idx_tid_bn_li`)",
		},
		{
			name: "address only",
			find: func(db *gorm.DB) error {
				filter := AddressIndexedScanLogFilter{
					scanLogFilter: scanLogFilter{TableName: "addr_logs_1", BlockTo: 100},
					ContractID:    1,
				}
				_, err := filter.Find(context.Background(), db, nil, false, 10)
				return err
			},
			forceHint: "FORCE INDEX (`idx_cid_bn_li`)",
		},
		{
			name: "address and topic0",
			find: func(db *gorm.DB) error {
				topicID := uint64(2)
				filter := AddressIndexedScanLogFilter{
					scanLogFilter: scanLogFilter{
						TableName: "addr_logs_1", BlockTo: 100, Topic0ID: &topicID,
					},
					ContractID: 1,
				}
				_, err := filter.Find(context.Background(), db, nil, false, 10)
				return err
			},
			forceHint: "FORCE INDEX (`idx_cid_tid_bn_li`)",
		},
		{
			name: "topic0 only",
			find: func(db *gorm.DB) error {
				filter := TopicIndexedScanLogFilter{
					scanLogFilter: scanLogFilter{TableName: "topic_logs_1", BlockTo: 100},
					TopicID:       2,
				}
				_, err := filter.Find(context.Background(), db, nil, false, 10)
				return err
			},
			forceHint: "FORCE INDEX (`idx_tid_bn_li`)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var sql string
			callbackName := "test:capture-force-index"
			require.NoError(t, db.Callback().Query().After("gorm:query").Register(
				callbackName, func(tx *gorm.DB) { sql = tx.Statement.SQL.String() },
			))
			t.Cleanup(func() { _ = db.Callback().Query().Remove(callbackName) })

			require.NoError(t, test.find(db))
			assert.Contains(t, sql, test.forceHint)
		})
	}
}

func newScanLogFilterTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	return db
}

func scanLogKeys(rows []*log) [][2]uint64 {
	keys := make([][2]uint64, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, [2]uint64{row.BlockNumber, row.LogIndex})
	}
	return keys
}
