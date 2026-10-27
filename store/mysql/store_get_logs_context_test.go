package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestIndexedGetLogsCancellation(t *testing.T) {
	for _, kind := range []string{"address", "topic"} {
		for _, stage := range []string{"before_query", "after_precheck", "connection_wait"} {
			t.Run(kind+"/"+stage, func(t *testing.T) {
				db := newScanLogFilterTestDB(t)
				sqlDB, err := db.DB()
				require.NoError(t, err)
				sqlDB.SetMaxOpenConns(1)
				t.Cleanup(func() { sqlDB.Close() })
				filter := LogFilter{TableName: "indexed_logs", BlockFrom: 0, BlockTo: 100}
				var find func(context.Context) error
				if kind == "address" {
					require.NoError(t, db.Table(filter.TableName).AutoMigrate(&AddressIndexedLog{}))
					require.NoError(t, db.Table(filter.TableName).Create(&AddressIndexedLog{ContractID: 1, BlockNumber: 10}).Error)
					find = func(ctx context.Context) error {
						_, err := (&AddressIndexedLogFilter{LogFilter: filter, ContractId: 1}).Find(ctx, db)
						return err
					}
				} else {
					require.NoError(t, db.Table(filter.TableName).AutoMigrate(&TopicIndexedLog{}))
					require.NoError(t, db.Table(filter.TableName).Create(&TopicIndexedLog{Topic0ID: 1, BlockNumber: 10}).Error)
					find = func(ctx context.Context) error {
						_, err := (&TopicIndexedLogFilter{LogFilter: filter, TopicId: 1}).Find(ctx, db)
						return err
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				switch stage {
				case "before_query":
					cancel()
				case "after_precheck":
					require.NoError(t, db.Callback().Query().After("gorm:query").Register("test:cancel", func(tx *gorm.DB) {
						if len(tx.Statement.Selects) > 0 {
							cancel()
						}
					}))
				case "connection_wait":
					conn, err := sqlDB.Conn(ctx)
					require.NoError(t, err)
					defer conn.Close()
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, 50*time.Millisecond)
					defer stop()
				}
				err = find(ctx)
				if stage == "connection_wait" {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
				// A canceled query must not attach its context to the shared DB.
				if stage != "connection_wait" {
					require.NoError(t, find(store.NewContextWithBoundChecksDisabled(context.Background())))
				}
			})
		}
	}
}
