//go:build integration

package mysql

import (
	"context"
	"errors"
	"testing"

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
