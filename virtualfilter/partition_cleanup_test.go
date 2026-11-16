package virtualfilter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	cmdutil "github.com/Conflux-Chain/confura/cmd/util"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestPartialPartitionCleanup(t *testing.T) {
	for _, rotate := range []bool{false, true} {
		for _, fault := range []string{"metadata", "rename", "cancel"} {
			t.Run(fmt.Sprintf("rotate=%v/%s", rotate, fault), func(t *testing.T) {
				db, store := testLogStore(t)
				_, _, err := store.PreparePartition("0xother")
				require.NoError(t, err)
				releaseSQLitePartitionIndex(t, db)
				index := 0
				if rotate {
					_, _, err = store.PreparePartition("0xpartial")
					require.NoError(t, err)
					require.NoError(t, db.Table("bn_partitions").Where("entity = ?", "vflogs_0xpartial").Update("count", 10000000).Error)
					index = 1
					releaseSQLitePartitionIndex(t, db)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				require.NoError(t, db.Callback().Create().Before("gorm:create").Register("partial_metadata", func(tx *gorm.DB) {
					if fault == "metadata" && tx.Statement.Table == "bn_partitions" {
						tx.AddError(errors.New("metadata unavailable"))
					}
				}))
				require.NoError(t, db.Callback().Raw().Before("gorm:raw").Register("partial_rename", func(tx *gorm.DB) {
					if fault == "rename" && strings.Contains(tx.Statement.SQL.String(), "RENAME") {
						tx.AddError(errors.New("rename unavailable"))
					}
				}))
				require.NoError(t, db.Callback().Raw().After("gorm:raw").Register("partial_cancel", func(tx *gorm.DB) {
					if fault == "cancel" && strings.Contains(tx.Statement.SQL.String(), "RENAME") {
						cancel()
					}
				}))
				_, _, err = store.WithContext(ctx).PreparePartition("0xpartial")
				require.Error(t, err)
				require.NoError(t, db.Callback().Create().Remove("partial_metadata"))
				require.NoError(t, db.Callback().Raw().Remove("partial_rename"))
				require.NoError(t, db.Callback().Raw().Remove("partial_cancel"))
				target := fmt.Sprintf("vflogs_0xpartial_%d", index)
				if fault == "rename" {
					target = "vflogs_0xpartial"
				}
				require.True(t, db.Migrator().HasTable(target), "fixture must leave an allocated table")
				require.NoError(t, db.Callback().Raw().Before("gorm:raw").Register("drop_fault", func(tx *gorm.DB) {
					if strings.HasPrefix(tx.Statement.SQL.String(), "DROP TABLE") {
						tx.AddError(errors.New("drop unavailable"))
					}
				}))
				require.ErrorContains(t, store.WithContext(context.Background()).DeletePartitions("0xpartial"), "drop unavailable")
				require.True(t, db.Migrator().HasTable(target))
				require.NoError(t, db.Callback().Raw().Remove("drop_fault"))
				require.NoError(t, store.WithContext(context.Background()).DeletePartitions("0xpartial"))
				for _, name := range []string{"vflogs_0xpartial", "vflogs_0xpartial_0", "vflogs_0xpartial_1"} {
					require.False(t, db.Migrator().HasTable(name), name)
				}
				var n int64
				require.NoError(t, db.Table("bn_partitions").Where("entity = ?", "vflogs_0xpartial").Count(&n).Error)
				require.Zero(t, n)
				require.True(t, db.Migrator().HasTable("vflogs_0xother_0"), "another session must survive")
				require.NoError(t, store.DeletePartitions("0xpartial"), "cleanup is idempotent")
			})
		}
	}
}

func TestPartitionCleanupRetainsMetadata(t *testing.T) {
	for _, operation := range []string{"close", "gc"} {
		for _, fault := range []string{"drop", "metadata"} {
			t.Run(operation+"/"+fault, func(t *testing.T) {
				db, store := testLogStore(t)
				count := 1
				if operation == "gc" {
					count = 3
				}
				for i := 0; i < count; i++ {
					releaseSQLitePartitionIndex(t, db)
					_, _, err := store.PreparePartition("0xretain")
					require.NoError(t, err)
					require.NoError(t, db.Table("bn_partitions").Where("entity = ?", "vflogs_0xretain").Update("count", 10000000).Error)
				}
				require.NoError(t, db.Callback().Raw().Before("gorm:raw").Register("retain_drop", func(tx *gorm.DB) {
					if fault == "drop" && strings.HasPrefix(tx.Statement.SQL.String(), "DROP TABLE") {
						tx.AddError(errors.New("drop unavailable"))
					}
				}))
				require.NoError(t, db.Callback().Delete().Before("gorm:delete").Register("retain_metadata", func(tx *gorm.DB) {
					if fault == "metadata" {
						tx.AddError(errors.New("metadata delete unavailable"))
					}
				}))
				if operation == "gc" {
					require.Error(t, store.GC("0xretain"))
				} else {
					require.Error(t, store.DeletePartitions("0xretain"))
				}
				var n int64
				require.NoError(t, db.Table("bn_partitions").Where("entity = ? AND pi = ?", "vflogs_0xretain", 0).Count(&n).Error)
				require.EqualValues(t, 1, n, "failed physical/metadata cleanup must keep a retry identity")
				require.NoError(t, db.Callback().Raw().Remove("retain_drop"))
				require.NoError(t, db.Callback().Delete().Remove("retain_metadata"))
				require.NoError(t, store.DeletePartitions("0xretain"))
				for i := 0; i < count; i++ {
					require.False(t, db.Migrator().HasTable(fmt.Sprintf("vflogs_0xretain_%d", i)))
				}
			})
		}
	}
}

func TestFailedMetadataCreationRetainsSessionCleanup(t *testing.T) {
	db, store := testLogStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	client := &resourceClient{}
	worker := newFilterWorker("eth", "fixture", client, &ethFilterSystem{filterSystemBase: &filterSystemBase{logStore: store}}, cmdutil.GracefulShutdownContext{Ctx: ctx, Wg: wg})
	defer func() { cancel(); wg.Wait() }()
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register("fail_metadata", func(tx *gorm.DB) {
		if tx.Statement.Table == "bn_partitions" {
			tx.AddError(errors.New("metadata unavailable"))
		}
	}))
	require.NoError(t, db.Callback().Raw().Before("gorm:raw").Register("fail_drop", func(tx *gorm.DB) {
		if strings.HasPrefix(tx.Statement.SQL.String(), "DROP TABLE") {
			tx.AddError(errors.New("drop unavailable"))
		}
	}))
	require.Error(t, worker.accept(newEthFilter("0x1", filterTypeLog, nil)))
	worker.mu.RLock()
	closing := worker.closing
	worker.mu.RUnlock()
	require.NotNil(t, closing)
	table := "vflogs_" + string(closing.fid) + "_0"
	require.True(t, db.Migrator().HasTable(table))
	worker.retryCleanup(ctx, true)
	worker.mu.RLock()
	pending := worker.closing != nil && !worker.closing.storeDone
	worker.mu.RUnlock()
	require.True(t, pending)
	require.NoError(t, db.Callback().Create().Remove("fail_metadata"))
	require.NoError(t, db.Callback().Raw().Remove("fail_drop"))
	worker.retryCleanup(ctx, true)
	worker.mu.RLock()
	done := worker.closing == nil
	worker.mu.RUnlock()
	require.True(t, done)
	require.False(t, db.Migrator().HasTable(table))
	require.EqualValues(t, 1, client.uninstalled.Load())
}

// MySQL index names are table-scoped; SQLite names are database-scoped. These
// DDL fixtures remove only SQLite's performance index before allocating another
// table. MySQL tests execute the production schema without this adjustment.
func releaseSQLitePartitionIndex(t *testing.T, db *gorm.DB) {
	t.Helper()
	if db.Dialector.Name() == "sqlite" {
		require.NoError(t, db.Exec("DROP INDEX IF EXISTS idx_bn").Error)
	}
}
