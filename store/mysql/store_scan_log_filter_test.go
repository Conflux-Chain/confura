package mysql

import (
	"context"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
