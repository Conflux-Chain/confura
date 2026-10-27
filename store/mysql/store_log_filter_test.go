package mysql

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestGetLogsReadsAllPages(t *testing.T) {
	for _, count := range []int{0, 1, 999, 1000, 1001, 10000, 10001, 10002} {
		db := newScanLogFilterTestDB(t)
		require.NoError(t, db.Exec(`CREATE TABLE paged_logs (id INTEGER PRIMARY KEY, bn INTEGER, epoch INTEGER, log_index INTEGER, cid INTEGER, tid INTEGER, topic0 TEXT, topic1 TEXT, topic2 TEXT, topic3 TEXT, extra BLOB)`).Error)
		// IDs run in the opposite direction to log order; a cursor must not use IDs.
		require.NoError(t, db.Exec(`WITH RECURSIVE seq(n) AS (SELECT 0 WHERE ? > 0 UNION ALL SELECT n+1 FROM seq WHERE n < ?) INSERT INTO paged_logs (id,bn,epoch,log_index,cid,tid,topic0,topic1) SELECT ?-n,10+n/6000,1,n%6000,7,8,'a','match' FROM seq`, count, count-1, count).Error)
		require.NoError(t, db.Exec(`INSERT INTO paged_logs (id,bn,log_index,cid,tid,topic0,topic1) VALUES (?,10,6000,99,8,'a','match'),(?,10,6001,7,99,'a','other')`, count+1, count+2).Error)
		ctx := store.NewContextWithBoundChecksDisabled(context.Background())
		filter := LogFilter{TableName: "paged_logs", BlockFrom: 10, BlockTo: 11, Topics: []store.VariadicValuer{store.NewVariadicValue("a"), store.NewVariadicValue("match")}}
		for _, path := range []string{"universal", "address", "topic", "contract", "dedicated_topic"} {
			t.Run(path+"/"+strconv.Itoa(count), func(t *testing.T) {
				var keys []store.ScanCursor
				var err error
				switch path {
				case "universal":
					var rows []*log
					err = queryLogsWithSizeValidation(ctx, db.Where("cid = ?", 7), &filter, int(store.MaxLogLimit)+1, &rows)
					keys = logRowKeys(rows)
				case "address":
					f := AddressIndexedLogFilter{LogFilter: filter, ContractId: 7}
					f.Schema = &PrimaryIDTopicSchema
					f.Topics = []store.VariadicValuer{store.NewVariadicValue(uint64(8)), store.NewVariadicValue("match")}
					var rows []*AddressIndexedLog
					rows, err = f.Find(ctx, db)
					keys = logRowKeys(rows)
				case "topic":
					f := TopicIndexedLogFilter{LogFilter: filter, TopicId: 8}
					f.Schema = &SecondaryOnlyTopicSchema
					var rows []*TopicIndexedLog
					rows, err = f.Find(ctx, db.Where("cid = ?", 7))
					keys = logRowKeys(rows)
				case "contract":
					var rows []*contractLog
					err = queryLogsWithSizeValidation(ctx, db.Where("cid = ?", 7), &filter, int(store.MaxLogLimit)+1, &rows)
					keys = logRowKeys(rows)
				case "dedicated_topic":
					var rows []*topicLog
					err = queryLogsWithSizeValidation(ctx, db.Where("cid = ?", 7), &filter, int(store.MaxLogLimit)+1, &rows)
					keys = logRowKeys(rows)
				}
				require.NoError(t, err)
				require.Len(t, keys, count)
				for i, key := range keys {
					require.Equal(t, store.ScanCursor{BlockNumber: uint64(10 + i/6000), LogIndex: uint64(i % 6000)}, key)
				}
			})
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		var rows []*log
		require.ErrorIs(t, queryLogsWithSizeValidation(cancelled, db, &filter, int(store.MaxLogLimit)+1, &rows), store.ErrGetLogsTimeout)
		require.Empty(t, rows)
	}
}

func logRowKeys[T interface{ logCursor() store.ScanCursor }](rows []T) []store.ScanCursor {
	keys := make([]store.ScanCursor, len(rows))
	for i, row := range rows {
		keys[i] = row.logCursor()
	}
	return keys
}

func TestGetLogsPageFailureDiscardsPartialResult(t *testing.T) {
	for _, cancelQuery := range []bool{false, true} {
		t.Run(strconv.FormatBool(cancelQuery), func(t *testing.T) {
			db := newScanLogFilterTestDB(t)
			require.NoError(t, db.Exec(`CREATE TABLE paged_logs (bn INTEGER, log_index INTEGER)`).Error)
			require.NoError(t, db.Exec(`WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n < 1000) INSERT INTO paged_logs SELECT 10,n FROM seq`).Error)
			ctx, cancel := context.WithCancel(store.NewContextWithBoundChecksDisabled(context.Background()))
			defer cancel()
			queryErr := errors.New("second page failed")
			pages := 0
			require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:page_failure", func(tx *gorm.DB) {
				pages++
				if pages == 2 && !cancelQuery {
					tx.AddError(queryErr)
				}
			}))
			require.NoError(t, db.Callback().Query().After("gorm:query").Register("test:cancel_after_page", func(tx *gorm.DB) {
				if pages == 1 && cancelQuery {
					cancel()
				}
			}))
			var rows []*log
			filter := LogFilter{TableName: "paged_logs", BlockFrom: 10, BlockTo: 10}
			err := queryLogRows(ctx, db, &filter, &rows)
			if cancelQuery {
				require.ErrorIs(t, err, store.ErrGetLogsTimeout)
			} else {
				require.ErrorIs(t, err, queryErr)
			}
			require.Nil(t, rows)
		})
	}
}
