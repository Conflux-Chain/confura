package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type getLogsQueryStats struct {
	queries  int
	bodyRows int64
}

// Each partition holds logs in one block, with disjoint, ordered block ranges.
func newGetLogsPartitionFixture(t testing.TB, counts ...int) (*logStore[store.EthData], store.LogFilter, *getLogsQueryStats) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&bnPartition{}))
	for i, count := range counts {
		from := int64(10 + i*100)
		require.NoError(t, db.Create(&bnPartition{
			Entity: bnPartitionedLogEntity, Index: uint32(i),
			BnMin: sql.NullInt64{Int64: from, Valid: true},
			BnMax: sql.NullInt64{Int64: from + 99, Valid: true},
		}).Error)
		table := fmt.Sprintf("logs_%d", i)
		// SQLite index names are global, unlike MySQL's per-table names.
		require.NoError(t, db.Exec(fmt.Sprintf(`CREATE TABLE %s (
			id INTEGER PRIMARY KEY, bn INTEGER, epoch INTEGER, log_index INTEGER,
			topic0 TEXT, topic1 TEXT, topic2 TEXT, topic3 TEXT, extra BLOB
		)`, table)).Error)
		require.NoError(t, db.Exec(fmt.Sprintf("CREATE INDEX %s_bn ON %s (bn, log_index)", table, table)).Error)
		rows := make([]*log, count)
		for j := range rows {
			rows[j] = &log{
				BlockNumber: uint64(from), Epoch: uint64(i + 1), LogIndex: uint64(j),
				Topic0: "event", Extra: []byte(strings.Repeat("x", 256)),
			}
		}
		if count > 0 {
			require.NoError(t, db.Table(table).CreateInBatches(rows, 100).Error)
		}
	}
	stats := new(getLogsQueryStats)
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("test:getlogs-stats", func(tx *gorm.DB) {
		stats.queries++
		if strings.HasPrefix(tx.Statement.Table, "logs_") && len(tx.Statement.Selects) == 0 {
			stats.bodyRows += tx.RowsAffected
		}
	}))
	return newLogStore[store.EthData](db, nil, nil, nil), store.LogFilter{
		BlockFrom: 10, BlockTo: uint64(10 + len(counts)*100 - 1),
	}, stats
}

func TestGetLogsResultBoundaries(t *testing.T) {
	for _, count := range []int{0, 9999, 10000, 10001} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ls, filter, _ := newGetLogsPartitionFixture(t, count)
			logs, err := ls.GetLogs(context.Background(), filter)
			if count > int(store.MaxLogLimit) {
				require.ErrorIs(t, err, store.ErrFilterResultSetTooLarge)
				return
			}
			require.NoError(t, err)
			require.Len(t, logs, count)
		})
	}
}

func BenchmarkGetLogsPartitions(b *testing.B) {
	for _, test := range []struct {
		name   string
		counts []int
		topic  bool
	}{
		{"small", []int{32}, false},
		{"at_limit", []int{10000}, false},
		{"oversized", []int{10001}, false},
		{"cross_partition", []int{9999, 10000}, false},
		{"with_topic", []int{10000}, true},
	} {
		b.Run(test.name, func(b *testing.B) {
			ls, filter, stats := newGetLogsPartitionFixture(b, test.counts...)
			if test.topic {
				filter.Topics = []store.VariadicValue[string]{store.NewVariadicValue("event")}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ls.GetLogs(context.Background(), filter)
				if err != nil && !errors.Is(err, store.ErrFilterResultSetTooLarge) {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(stats.queries)/float64(b.N), "queries/op")
			b.ReportMetric(float64(stats.bodyRows)/float64(b.N), "body_rows/op")
		})
	}
}
