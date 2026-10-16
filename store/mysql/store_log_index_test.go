package mysql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type sqliteIndexListRow struct {
	Name string
}

type sqliteIndexInfoRow struct {
	Name string
}

func TestNewLogPartitionsUseScanLogsIndexes(t *testing.T) {
	tests := []struct {
		name       string
		model      schema.Tabler
		table      string
		expected   map[string][]string
		replacedBy []string
	}{
		{
			name:     "universal logs",
			model:    &log{},
			table:    "logs_0",
			expected: map[string][]string{"idx_bn_li": {"bn", "log_index"}},
			replacedBy: []string{
				"idx_bn",
			},
		},
		{
			name:  "address indexed logs",
			model: &AddressIndexedLog{},
			table: "addr_logs_0",
			expected: map[string][]string{
				"idx_cid_bn_li":     {"cid", "bn", "log_index"},
				"idx_cid_tid_bn_li": {"cid", "tid", "bn", "log_index"},
			},
			replacedBy: []string{"idx_cid_bn", "idx_cid_tid_bn"},
		},
		{
			name:  "topic indexed logs",
			model: &TopicIndexedLog{},
			table: "topic_logs_0",
			expected: map[string][]string{
				"idx_tid_bn_li": {"tid", "bn", "log_index"},
			},
			replacedBy: []string{"idx_tid_bn"},
		},
		{
			name:  "dedicated contract logs",
			model: &contractLog{ContractID: 42},
			table: "clogs_42_0",
			expected: map[string][]string{
				"idx_bn_li":     {"bn", "log_index"},
				"idx_tid_bn_li": {"tid", "bn", "log_index"},
			},
			replacedBy: []string{"idx_bn", "idx_tid_bn"},
		},
		{
			name:  "dedicated topic logs",
			model: &topicLog{Topic0ID: 7},
			table: "tlogs_7_0",
			expected: map[string][]string{
				"idx_bn_li": {"bn", "log_index"},
			},
			replacedBy: []string{"idx_bn"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
			require.NoError(t, err)

			created, err := (&partitionedStore{}).createPartitionedTable(db, test.model, 0)
			require.NoError(t, err)
			require.True(t, created)

			indexes := loadSQLiteIndexes(t, db, test.table)
			for name, columns := range test.expected {
				require.Equal(t, columns, indexes[name], "index %s on table %s", name, test.table)
			}
			for _, oldName := range test.replacedBy {
				require.NotContains(t, indexes, oldName, "old index %s on table %s", oldName, test.table)
			}
		})
	}
}

func loadSQLiteIndexes(t *testing.T, db *gorm.DB, table string) map[string][]string {
	t.Helper()

	var indexRows []sqliteIndexListRow
	require.NoError(t, db.Raw(fmt.Sprintf("PRAGMA index_list(%q)", table)).Scan(&indexRows).Error)

	indexes := make(map[string][]string, len(indexRows))
	for _, index := range indexRows {
		var columnRows []sqliteIndexInfoRow
		require.NoError(t, db.Raw(fmt.Sprintf("PRAGMA index_info(%q)", index.Name)).Scan(&columnRows).Error)

		columns := make([]string, 0, len(columnRows))
		for _, column := range columnRows {
			columns = append(columns, column.Name)
		}
		indexes[index.Name] = columns
	}

	return indexes
}
