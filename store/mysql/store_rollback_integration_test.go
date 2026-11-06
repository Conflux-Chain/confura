//go:build integration

package mysql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestMySQLRollbackCrossPartition(t *testing.T) {
	for _, kind := range []string{"universal", "contract", "topic"} {
		t.Run(kind, func(t *testing.T) {
			ms, db := newIntegrationStore(t, false)
			require.NoError(t, ms.Push(integrationData(1, 2)))
			if kind != "universal" {
				if kind == "contract" {
					require.NoError(t, db.Model(&Contract{}).Where("id > 0").Update("log_count", thresholdBigContractLogCount).Error)
				} else {
					require.NoError(t, db.Model(&Topic{}).Where("id > 0").Update("log_count", thresholdBigTopicLogCount).Error)
				}
				require.NoError(t, ms.Push(integrationData(2, 2)))
			}
			entity := "logs"
			var model interface{ TableName() string } = &log{}
			if kind == "contract" {
				entity = ms.bcls.contractEntity(1)
				model = ms.bcls.contractTabler(1)
			}
			if kind == "topic" {
				entity = ms.btls.topicEntity(1)
				model = ms.btls.topicTabler(1)
			}
			_, e := ms.ls.growPartition(entity, model)
			require.NoError(t, e)
			n := uint64(2)
			if kind != "universal" {
				n = 3
			}
			require.NoError(t, ms.Push(integrationData(n, 2)))
			var before []bnPartition
			require.NoError(t, db.Where("entity = ?", entity).Order("pi").Find(&before).Error)
			beforeCounts := map[string]int64{}
			for _, table := range []string{"logs_0", entity + "_0", entity + "_1"} {
				beforeCounts[table] = integrationCount(t, db, table)
			}
			failure := errors.New("reject rollback")
			require.ErrorIs(t, ms.PopnWithFinalizer(1, func(*gorm.DB) error { return failure }), failure)
			maxEpoch, ok, e := ms.MaxEpoch()
			require.NoError(t, e)
			require.True(t, ok)
			require.Equal(t, n, maxEpoch)
			var after []bnPartition
			require.NoError(t, db.Where("entity = ?", entity).Order("pi").Find(&after).Error)
			require.Equal(t, before, after)
			for table, count := range beforeCounts {
				require.Equal(t, count, integrationCount(t, db, table))
			}
			require.Error(t, db.Transaction(func(tx *gorm.DB) error {
				return ms.ls.deltaUpdateCount(tx, entity, 0, 1)
			}))
			// Clearing the newest partition must not prevent a later rollback of older rows.
			require.NoError(t, ms.Popn(n))
			require.NoError(t, ms.Popn(1))
			_, ok, e = ms.MaxEpoch()
			require.NoError(t, e)
			require.False(t, ok)
			for _, table := range []string{"logs_0", entity + "_0", entity + "_1"} {
				require.Zero(t, integrationCount(t, db, table))
			}
			var partitions []bnPartition
			require.NoError(t, db.Where("entity = ?", entity).Find(&partitions).Error)
			require.Len(t, partitions, 2)
			for _, p := range partitions {
				require.Zero(t, p.Count)
				require.False(t, p.BnMin.Valid)
				require.False(t, p.BnMax.Valid)
			}
			require.NoError(t, ms.Push(integrationData(1, 0)))
		})
	}
}
