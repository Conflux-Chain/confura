//go:build integration

package mysql

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestMySQLMigrationSurvivesEmptyRollback(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "legacy"}[legacy], func(t *testing.T) {
			ms, db := newIntegrationStore(t, false)
			require.NoError(t, ms.Push(integrationData(1, 4)))
			for _, model := range []interface{}{&Contract{}, &Topic{}} {
				require.NoError(t, db.Model(model).Where("id > 0").Update("log_count", 100000).Error)
			}
			require.NoError(t, ms.Push(integrationData(2, 4)))
			if legacy { // An old writer never stored the new field.
				for _, model := range []interface{}{&Contract{}, &Topic{}} {
					require.NoError(t, db.Model(model).Where("id > 0").Update("migration_status", migrationLegacy).Error)
				}
			}
			require.NoError(t, ms.Popn(1))
			done, err := ms.bcls.IsMigrationCompleted(1)
			require.NoError(t, err)
			require.True(t, done)
			done, err = ms.btls.IsMigrationCompleted(1)
			require.NoError(t, err)
			require.True(t, done)
			// Simulate a restart/cache eviction and replay after every log was removed.
			ms.cs.cacheById.Purge()
			ms.cs.cacheByAddress.Purge()
			ms.ts.idCache.Purge()
			ms.ts.hashCache.Purge()
			require.NoError(t, ms.Push(integrationData(1, 4)))
			for _, table := range []string{"clogs_1_0", "clogs_2_0", "tlogs_1_0", "tlogs_2_0"} {
				require.EqualValues(t, 2, integrationCount(t, db, table))
			}
			for _, table := range []string{"addr_logs_0", "addr_logs_1", "topic_logs_0", "topic_logs_1"} {
				require.Zero(t, integrationCount(t, db, table))
			}
			var contract Contract
			require.NoError(t, db.First(&contract).Error)
			logs, err := ms.GetLogs(context.Background(), store.LogFilter{BlockFrom: 1, BlockTo: 1, Contracts: store.NewVariadicValue(contract.Address)})
			require.NoError(t, err)
			require.Len(t, logs, 2)
		})
	}
}

func TestMySQLMigrationStatusFailureRollsBackCopy(t *testing.T) {
	ms, db := newIntegrationStore(t, false)
	require.NoError(t, ms.Push(integrationData(1, 4)))
	require.NoError(t, db.Model(&Contract{}).Where("id > 0").Update("log_count", 100000).Error)
	failure := errors.New("migration marker update failed")
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register("fail-migration-marker", func(tx *gorm.DB) {
		if tx.Statement.Table == "contracts" {
			tx.AddError(failure)
		}
	}))
	require.ErrorIs(t, ms.Push(integrationData(2, 4)), failure)
	require.NoError(t, db.Callback().Update().Remove("fail-migration-marker"))
	require.EqualValues(t, 4, integrationCount(t, db, "addr_logs_0")+integrationCount(t, db, "addr_logs_1"))
	var contracts []Contract
	require.NoError(t, db.Find(&contracts).Error)
	for _, c := range contracts {
		require.Equal(t, migrationShared, c.MigrationStatus)
	}
	require.NoError(t, ms.Push(integrationData(2, 4)))
}

func TestMySQLMigrationSchemaUpgrade(t *testing.T) {
	db, _ := newIntegrationDB(t)
	require.NoError(t, db.Migrator().DropColumn(&Contract{}, "migration_status"))
	require.ErrorContains(t, validateLogMigrationSchema(db), "schema upgrade required")
	// Old code inserts only the columns it knows. The additive default preserves that operation.
	type oldContract struct {
		ID                 uint64
		Address            string
		LogCount           int
		LatestUpdatedEpoch uint64
	}
	old := oldContract{Address: "old-contract"}
	require.NoError(t, db.Table("contracts").Create(&old).Error)
	require.NoError(t, db.Migrator().AddColumn(&Contract{}, "migration_status"))
	require.NoError(t, validateLogMigrationSchema(db))
	var current Contract
	require.NoError(t, db.First(&current, old.ID).Error)
	require.Equal(t, migrationLegacy, current.MigrationStatus)
	require.NoError(t, db.Table("contracts").Where("id = ?", old.ID).Update("log_count", 12).Error)
	require.NoError(t, db.Table("contracts").First(&old, old.ID).Error)
	require.Equal(t, 12, old.LogCount)
}

func TestMySQLConcurrentLogMigration(t *testing.T) {
	for _, kind := range []string{"contract", "topic"} {
		t.Run(kind, func(t *testing.T) {
			ms, db := newIntegrationStore(t, false)
			require.NoError(t, ms.Push(integrationData(1, 2002)))
			var model interface{} = &Contract{}
			entity, source, column, target := "clogs_1", "", "cid", "clogs_1_0"
			var migrate func() error
			if kind == "contract" {
				var c Contract
				require.NoError(t, db.First(&c, 1).Error)
				source = ms.ails.GetPartitionedTableName(c.Address)
				p, err := ms.bcls.growPartition(entity, ms.bcls.contractTabler(c.ID))
				require.NoError(t, err)
				migrate = func() error { return ms.bcls.migrate(&c, *p) }
			} else {
				model = &Topic{}
				entity = "tlogs_1"
				column = "tid"
				target = "tlogs_1_0"
				var topic Topic
				require.NoError(t, db.First(&topic, 1).Error)
				source = ms.tils.GetPartitionedTableName(topic.Hash)
				p, err := ms.btls.growPartition(entity, ms.btls.topicTabler(topic.ID))
				require.NoError(t, err)
				migrate = func() error { return ms.btls.migrate(&topic, *p) }
			}
			copied, release, secondLock := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			defer unblock.Do(func() { close(release) })
			var reads, locks atomic.Int32
			const queryHook = "migration-interleave"
			require.NoError(t, db.Callback().Query().After("gorm:query").Register(queryHook, func(tx *gorm.DB) {
				if tx.Statement.Table != source {
					return
				}
				switch tx.Statement.Dest.(type) {
				case *[]*AddressIndexedLog, *[]*TopicIndexedLog:
					if reads.Add(1) == 1 {
						close(copied)
						<-release
					}
				}
			}))
			defer db.Callback().Query().Remove(queryHook)
			const lockHook = "migration-lock-attempt"
			require.NoError(t, db.Callback().Query().Before("gorm:query").Register(lockHook, func(tx *gorm.DB) {
				if _, ok := tx.Statement.Clauses["FOR"]; ok && (tx.Statement.Table == "contracts" || tx.Statement.Table == "topics") && locks.Add(1) == 2 {
					close(secondLock)
				}
			}))
			defer db.Callback().Query().Remove(lockHook)
			results := make(chan error, 2)
			go func() { results <- migrate() }()
			select {
			case <-copied:
			case <-time.After(3 * time.Second):
				t.Fatal("first migration did not reach copy")
			}
			go func() { results <- migrate() }()
			select {
			case <-secondLock:
			case <-time.After(3 * time.Second):
				t.Fatal("second migration did not attempt lock")
			}
			select {
			case err := <-results:
				t.Fatalf("migration completed while first copy was paused: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			require.EqualValues(t, 1, reads.Load())
			// Readers still see the complete shared side while the migration is open.
			done, err := logMigrationCompleted(db, model, 1, entity)
			require.NoError(t, err)
			require.False(t, done)
			var n int64
			require.NoError(t, db.Table(source).Where(column+" = ?", 1).Count(&n).Error)
			require.EqualValues(t, 1001, n)
			unblock.Do(func() { close(release) })
			for i := 0; i < 2; i++ {
				select {
				case err := <-results:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Fatal("migration failed to finish")
				}
			}
			require.EqualValues(t, 2, reads.Load())
			require.EqualValues(t, 1001, integrationCount(t, db, target))
			require.NoError(t, db.Table(source).Where(column+" = ?", 1).Count(&n).Error)
			require.Zero(t, n)
			done, err = logMigrationCompleted(db, model, 1, entity)
			require.NoError(t, err)
			require.True(t, done)
			// The ordinary writer must reject a shared route prepared before migration.
			err = db.Transaction(func(tx *gorm.DB) error {
				return ms.lockLogWriteRoutes(tx, []*store.EthData{integrationData(2, 4)}, nil, nil)
			})
			require.ErrorContains(t, err, "log route changed")
			// Failure of a later batch does not undo the migration or duplicate its rows.
			failure := errors.New("reject batch")
			require.ErrorIs(t, ms.PushnWithFinalizer([]*store.EthData{integrationData(2, 4)}, func(*gorm.DB) error { return failure }), failure)
			require.EqualValues(t, 1001, integrationCount(t, db, target))
		})
	}
}
