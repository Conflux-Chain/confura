package handler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/store/mysql"
	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	web3types "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newGetLogsTimeoutFixture(t *testing.T, space string, attempts int, singleBlock bool) (*gorm.DB, func(context.Context) error) {
	t.Helper()
	db := newScanLogsHandlerTestDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { sqlDB.Close() })
	insertScanLogsMapping(t, db, 1, 10, 30, "")
	require.NoError(t, db.Exec(`CREATE TABLE bn_partitions (
		id INTEGER PRIMARY KEY, entity TEXT, pi INTEGER, count INTEGER,
		bn_min INTEGER, bn_max INTEGER, created_at DATETIME, updated_at DATETIME
	)`).Error)
	to := uint64(30)
	if singleBlock {
		to = 10
	}
	if space == "core" {
		h := NewCfxLogsApiHandler(mysql.NewCfxStore(db, &mysql.Config{}, store.StoreConfig()), nil, attempts)
		filter := &cfxtypes.LogFilter{FromBlock: cfxtypes.NewBigInt(10), ToBlock: cfxtypes.NewBigInt(to)}
		return db, func(ctx context.Context) error {
			_, _, err := h.GetLogs(ctx, nil, filter, "")
			return err
		}
	}
	h := NewEthLogsApiHandler(mysql.NewEthStore(db, &mysql.Config{}, store.EthStoreConfig()), attempts)
	fromBlock, toBlock := web3types.BlockNumber(10), web3types.BlockNumber(to)
	filter := &web3types.FilterQuery{FromBlock: &fromBlock, ToBlock: &toBlock}
	return db, func(ctx context.Context) error {
		_, _, err := h.GetLogs(ctx, nil, filter, "")
		return err
	}
}

func TestGetLogsSuggestionRetriesShareDeadline(t *testing.T) {
	for _, space := range []string{"core", "espace"} {
		for _, attempts := range []int{0, 1, 3} {
			t.Run(fmt.Sprintf("%s/attempts_%d", space, attempts), func(t *testing.T) {
				db, run := newGetLogsTimeoutFixture(t, space, attempts, false)
				var deadlines []time.Time
				require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:suggest", func(tx *gorm.DB) {
					if tx.Statement.Table != "bn_partitions" {
						return
					}
					deadline, ok := tx.Statement.Context.Deadline()
					require.True(t, ok)
					deadlines = append(deadlines, deadline)
					if len(deadlines) <= 2 {
						tx.AddError(store.NewSuggestedFilterOversizeError(store.ErrFilterResultSetTooLarge,
							store.NewSuggestedBlockRange(10, uint64(30-len(deadlines)*5), 1)))
					}
				}))
				err := run(context.Background())
				require.ErrorIs(t, err, store.ErrFilterResultSetTooLarge)
				var suggestion *store.SuggestedFilterOversizedError[store.SuggestedBlockRange]
				require.ErrorAs(t, err, &suggestion)
				wantCalls, wantEnd := 3, uint64(20)
				if attempts == 0 {
					wantCalls, wantEnd = 1, 25
				} else if attempts == 1 {
					wantCalls = 2
				}
				require.Len(t, deadlines, wantCalls)
				require.Equal(t, wantEnd, suggestion.SuggestedRange.To)
				for _, deadline := range deadlines {
					require.Equal(t, deadlines[0], deadline)
				}
			})
		}
	}
}

func TestGetLogsReorgRetriesShareDeadline(t *testing.T) {
	for _, space := range []string{"core", "espace"} {
		t.Run(space, func(t *testing.T) {
			db, run := newGetLogsTimeoutFixture(t, space, 0, false)
			var deadlines []time.Time
			versionReads := 0
			require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:reorg", func(tx *gorm.DB) {
				if tx.Statement.Table == "configs" {
					versionReads++
					if versionReads == 2 {
						require.NoError(t, db.Exec("INSERT INTO configs (name, value) VALUES (?, ?)", mysql.MysqlConfKeyReorgVersion, "1").Error)
					}
				}
				if tx.Statement.Table == "bn_partitions" {
					deadline, ok := tx.Statement.Context.Deadline()
					require.True(t, ok)
					deadlines = append(deadlines, deadline)
				}
			}))
			require.NoError(t, run(context.Background()))
			require.Len(t, deadlines, 2)
			require.Equal(t, deadlines[0], deadlines[1])
		})
	}
}

func TestGetLogsDatabaseDeadlineAndCancellation(t *testing.T) {
	for _, space := range []string{"core", "espace"} {
		for _, singleBlock := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/single_block_%t", space, singleBlock), func(t *testing.T) {
				db, run := newGetLogsTimeoutFixture(t, space, 3, singleBlock)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				wantDeadline, _ := ctx.Deadline()
				calls := 0
				require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:cancel", func(tx *gorm.DB) {
					if tx.Statement.Table != "bn_partitions" {
						return
					}
					calls++
					deadline, ok := tx.Statement.Context.Deadline()
					require.True(t, ok)
					require.Equal(t, wantDeadline, deadline)
					require.Equal(t, !singleBlock, store.IsBoundChecksEnabled(tx.Statement.Context))
					if !singleBlock && calls == 1 {
						tx.AddError(store.NewSuggestedFilterOversizeError(store.ErrFilterResultSetTooLarge,
							store.NewSuggestedBlockRange(10, 25, 1)))
						return
					}
					cancel()
				}))
				require.ErrorIs(t, run(ctx), store.ErrGetLogsTimeout)
				if singleBlock {
					require.Equal(t, 1, calls)
				} else {
					require.Equal(t, 2, calls)
				}
			})
		}
	}
}
