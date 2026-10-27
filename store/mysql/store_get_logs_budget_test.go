package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestGetLogsOrderedPartitionBudget(t *testing.T) {
	for _, kind := range []string{"universal", "contract", "topic"} {
		for _, withTopic := range []bool{false, true} {
			for _, test := range []struct {
				name         string
				counts       []int
				disableBound bool
				wantRows     []int64
				oversized    bool
			}{
				{"remaining_two", []int{9999, 10000, 1}, false, []int64{9999, 2}, true},
				{"probe_after_limit", []int{10000, 1, 1}, false, []int64{10000, 1}, true},
				{"empty_middle", []int{9999, 0, 1}, false, []int64{9999, 0, 1}, false},
				{"empty_tail", []int{10000, 0}, false, []int64{10000, 0}, false},
				{"checks_disabled", []int{9999, 10000, 1}, true, []int64{9999, 10000, 1}, false},
			} {
				t.Run(fmt.Sprintf("%s/topic_%t/%s", kind, withTopic, test.name), func(t *testing.T) {
					ls, filter, _ := newGetLogsPartitionFixture(t, test.counts...)
					db := ls.db
					ts := NewTopicStore(db)
					ts.cache(&Topic{ID: 1, Hash: "event"})
					entity := "logs"
					var query func(context.Context) ([]*store.Log, error)
					switch kind {
					case "universal":
						query = func(ctx context.Context) ([]*store.Log, error) { return ls.GetLogs(ctx, filter) }
					case "contract":
						bcls := newBigContractLogStore[store.EthData](db, nil, ts, nil, nil, nil)
						entity = bcls.contractEntity(1)
						query = func(ctx context.Context) ([]*store.Log, error) { return bcls.GetContractLogs(ctx, 1, filter) }
					case "topic":
						btls := newBigTopicLogStore[store.EthData](db, ts, nil, nil, nil)
						entity = btls.topicEntity(1)
						query = func(ctx context.Context) ([]*store.Log, error) { return btls.GetTopicLogs(ctx, 1, "event", filter) }
					}
					if entity != "logs" {
						require.NoError(t, db.Model(&bnPartition{}).Where("entity = ?", "logs").Update("entity", entity).Error)
						for i := range test.counts {
							table := fmt.Sprintf("%s_%d", entity, i)
							require.NoError(t, db.Migrator().RenameTable(fmt.Sprintf("logs_%d", i), table))
							require.NoError(t, db.Exec("ALTER TABLE "+table+" ADD COLUMN tid INTEGER DEFAULT 1").Error)
						}
					}
					if withTopic {
						filter.Topics = []store.VariadicValue[string]{store.NewVariadicValue("event")}
					}
					var bodyRows []int64
					lastTable := ""
					require.NoError(t, db.Callback().Query().After("gorm:query").Register("test:budget", func(tx *gorm.DB) {
						if strings.HasPrefix(tx.Statement.Table, entity+"_") && len(tx.Statement.Selects) == 0 {
							if lastTable != tx.Statement.Table {
								lastTable = tx.Statement.Table
								bodyRows = append(bodyRows, 0)
							}
							bodyRows[len(bodyRows)-1] += tx.RowsAffected
						}
					}))
					ctx := context.Background()
					if test.disableBound {
						ctx = store.NewContextWithBoundChecksDisabled(ctx)
					}
					logs, err := query(ctx)
					require.Equal(t, test.wantRows, bodyRows)
					if test.oversized {
						require.ErrorIs(t, err, store.ErrFilterResultSetTooLarge)
						var suggestion *store.SuggestedFilterOversizedError[store.SuggestedBlockRange]
						require.ErrorAs(t, err, &suggestion)
						require.EqualValues(t, 10, suggestion.SuggestedRange.From)
						require.EqualValues(t, 109, suggestion.SuggestedRange.To)
						require.EqualValues(t, 2, suggestion.SuggestedRange.MaxEndEpoch)
						return
					}
					require.NoError(t, err)
					count := 0
					for _, n := range test.counts {
						count += n
					}
					require.Len(t, logs, count)
					for i := 1; i < len(logs); i++ {
						require.LessOrEqual(t, logs[i-1].BlockNumber, logs[i].BlockNumber)
					}
				})
			}
		}
	}
}
