//go:build integration

package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestMySQLMigrationSurvivesEmptyRollback(t *testing.T) {
	ms, db := newIntegrationStore(t, false)
	require.NoError(t, ms.Push(integrationData(1, 4)))
	for _, model := range []interface{}{&Contract{}, &Topic{}} {
		require.NoError(t, db.Model(model).Where("id > 0").Update("log_count", 100000).Error)
	}
	require.NoError(t, ms.Push(integrationData(2, 4)))
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
}

func TestMySQLMigrationStateDefaults(t *testing.T) {
	db, _ := newIntegrationDB(t)
	cs, ts := NewContractStore(db), NewTopicStore(db)
	_, _, err := cs.AddContractIfAbsent("single-contract")
	require.NoError(t, err)
	_, _, err = ts.GetOrCreate("single-topic")
	require.NoError(t, err)
	_, err = cs.AddContract(map[string]bool{"batch-contract-1": true, "batch-contract-2": true})
	require.NoError(t, err)
	_, err = ts.BatchAdd(map[string]bool{"batch-topic-1": true, "batch-topic-2": true})
	require.NoError(t, err)
	for _, table := range []string{"contracts", "topics"} {
		require.EqualValues(t, 3, integrationCount(t, db, table))
		require.False(t, db.Migrator().HasColumn(table, "migration_status"))
	}
	require.Zero(t, integrationCount(t, db, "bn_partitions"))
	require.False(t, db.Migrator().HasTable("log_storage_routes"))
}

func TestMySQLMigrationRouteIsAuthoritative(t *testing.T) {
	ms, db := newIntegrationStore(t, false)
	require.NoError(t, ms.Push(integrationData(1, 4)))
	contract, ok, err := ms.cs.GetContractById(1)
	require.NoError(t, err)
	require.True(t, ok)
	topic, ok, err := ms.ts.GetTopicById(1)
	require.NoError(t, err)
	require.True(t, ok)
	for _, entity := range []string{"clogs_1", "tlogs_1"} {
		t.Run(entity, func(t *testing.T) {
			routes := newBnPartitionedStore(db)
			done, err := routes.hasPartition(entity)
			require.NoError(t, err)
			require.False(t, done)
			failure := errors.New("rollback route")
			err = db.Transaction(func(tx *gorm.DB) error {
				routesTx := newBnPartitionedStore(tx)
				require.NoError(t, tx.Create(&bnPartition{Entity: entity}).Error)
				done, err := routesTx.hasPartition(entity)
				require.NoError(t, err)
				require.True(t, done)
				done, err = routes.hasPartition(entity)
				require.NoError(t, err)
				require.False(t, done)
				return failure
			})
			require.ErrorIs(t, err, failure)
			done, err = routes.hasPartition(entity)
			require.NoError(t, err)
			require.False(t, done)
			require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
				return tx.Create(&bnPartition{Entity: entity}).Error
			}))
			done, err = routes.hasPartition(entity)
			require.NoError(t, err)
			require.True(t, done)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = newBnPartitionedStore(db.WithContext(ctx)).hasPartition(entity)
			require.ErrorIs(t, err, context.Canceled)
			_, err = newBnPartitionedStore(db.WithContext(ctx)).partitionedEntities([]string{entity})
			require.ErrorIs(t, err, context.Canceled)
		})
	}
	byID, _ := ms.cs.cacheById.Peek(contract.ID)
	byAddress, _ := ms.cs.cacheByAddress.Peek(contract.Address)
	require.Same(t, contract, byID)
	require.Same(t, contract, byAddress)
	byTopicID, _ := ms.ts.idCache.Peek(topic.ID)
	byHash, _ := ms.ts.hashCache.Peek(topic.Hash)
	require.Same(t, topic, byTopicID)
	require.Same(t, topic, byHash)
	routes := newBnPartitionedStore(db)
	dedicated, err := routes.partitionedEntities([]string{"clogs_1", "tlogs_1", "clogs_999", "clogs_1"})
	require.NoError(t, err)
	require.Equal(t, map[string]bool{"clogs_1": true, "tlogs_1": true}, dedicated)
	dedicated, err = routes.partitionedEntities(nil)
	require.NoError(t, err)
	require.Empty(t, dedicated)
	require.NoError(t, db.Migrator().DropTable(&bnPartition{}))
	_, err = routes.hasPartition("clogs_1")
	require.Error(t, err)
	_, err = routes.partitionedEntities([]string{"clogs_1"})
	require.Error(t, err)
}

func TestMySQLMigrationFailureRollsBackCopy(t *testing.T) {
	for _, kind := range []string{"contract", "topic"} {
		for _, stage := range []string{"ddl", "copy", "marker", "metadata", "mapping"} {
			t.Run(kind+"/"+stage, func(t *testing.T) {
				f := newIntegrationLogMigration(t, kind, 2002)
				failure := errors.New("injected migration failure")
				var writes int
				switch stage {
				case "ddl":
					require.NoError(t, f.db.Callback().Raw().Before("gorm:raw").Register("fail-ddl", func(tx *gorm.DB) {
						if strings.HasPrefix(tx.Statement.SQL.String(), "CREATE TABLE `"+f.entity+"`") {
							tx.AddError(failure)
						}
					}))
				case "copy":
					require.NoError(t, f.db.Callback().Create().Before("gorm:create").Register("fail-copy", func(tx *gorm.DB) {
						if tx.Statement.Table == f.target {
							writes++
							if writes == 2 {
								tx.AddError(failure)
							}
						}
					}))
				case "marker":
					require.NoError(t, f.db.Callback().Create().Before("gorm:create").Register("fail-marker", func(tx *gorm.DB) {
						if tx.Statement.Table == "bn_partitions" {
							tx.AddError(failure)
						}
					}))
				case "metadata":
					require.NoError(t, f.db.Callback().Update().Before("gorm:update").Register("fail-metadata", func(tx *gorm.DB) {
						if tx.Statement.Table == "bn_partitions" {
							tx.AddError(failure)
						}
					}))
				case "mapping":
					require.NoError(t, f.db.Exec("DELETE FROM epoch_block_map").Error)
				}
				err := f.migrate()
				if stage == "mapping" {
					require.ErrorContains(t, err, "earliest block mapping not found")
				} else {
					require.ErrorIs(t, err, failure)
				}
				if stage == "ddl" {
					require.NoError(t, f.db.Callback().Raw().Remove("fail-ddl"))
				} else if stage == "copy" {
					require.Equal(t, 2, writes)
					require.NoError(t, f.db.Callback().Create().Remove("fail-copy"))
				} else if stage == "marker" {
					require.NoError(t, f.db.Callback().Create().Remove("fail-marker"))
				} else if stage == "metadata" {
					require.NoError(t, f.db.Callback().Update().Remove("fail-metadata"))
				} else {
					require.NoError(t, f.db.Create(testEpochBlockMapping(1, 1, 1)).Error)
				}
				var sourceCount int64
				require.NoError(t, f.db.Table(f.source).Where(f.column+" = ?", 1).Count(&sourceCount).Error)
				require.EqualValues(t, 1001, sourceCount)
				if stage == "ddl" {
					require.False(t, f.db.Migrator().HasTable(f.target))
				} else {
					require.Zero(t, integrationCount(t, f.db, f.target))
				}
				status, err := newBnPartitionedStore(f.db).hasPartition(f.entity)
				require.NoError(t, err)
				require.False(t, status)
				filter := store.LogFilter{BlockFrom: 1, BlockTo: 1}
				if kind == "contract" {
					contract, _, err := f.ms.cs.GetContractById(1)
					require.NoError(t, err)
					filter.Contracts = store.NewVariadicValue(contract.Address)
				} else {
					topic, _, err := f.ms.ts.GetTopicById(1)
					require.NoError(t, err)
					filter.Topics = []store.VariadicValue[string]{store.NewVariadicValue(topic.Hash)}
				}
				logs, err := f.ms.GetLogs(context.Background(), filter)
				require.NoError(t, err)
				require.Len(t, logs, 1001)
				var partition bnPartition
				require.ErrorIs(t, f.db.Where("entity = ?", f.entity).Take(&partition).Error, gorm.ErrRecordNotFound)
				require.NoError(t, f.migrate())
				require.EqualValues(t, 1001, integrationCount(t, f.db, f.target))
			})
		}
	}
}

func TestMySQLMigrationQueryBudget(t *testing.T) {
	for _, kind := range []string{"contract", "topic"} {
		for _, empty := range []bool{false, true} {
			name := kind + "/populated"
			if empty {
				name = kind + "/empty"
			}
			t.Run(name, func(t *testing.T) {
				f := newIntegrationLogMigration(t, kind, 4)
				if empty {
					require.NoError(t, f.db.Exec("DELETE FROM "+f.source+" WHERE "+f.column+" = ?", 1).Error)
					require.NoError(t, f.db.Exec("DELETE FROM epoch_block_map").Error)
				}
				var routeReads, sourceReads, mappingReads int
				require.NoError(t, f.db.Callback().Query().After("gorm:query").Register("migration-budget", func(tx *gorm.DB) {
					switch tx.Statement.Table {
					case "bn_partitions":
						routeReads++
					case f.source:
						sourceReads++
						require.NotContains(t, strings.ToLower(tx.Statement.SQL.String()), "count(")
					case "epoch_block_map":
						mappingReads++
					}
				}))
				require.NoError(t, f.migrate())
				require.Equal(t, 1, routeReads)
				require.Equal(t, 1, sourceReads)
				if empty {
					require.Zero(t, mappingReads)
				} else {
					require.Equal(t, 1, mappingReads)
				}
				routeReads, sourceReads, mappingReads = 0, 0, 0
				require.NoError(t, f.migrate())
				require.Equal(t, 1, routeReads)
				require.Zero(t, sourceReads)
				require.Zero(t, mappingReads)
				require.NoError(t, f.db.Callback().Query().Remove("migration-budget"))
				status, err := newBnPartitionedStore(f.db).hasPartition(f.entity)
				require.NoError(t, err)
				require.True(t, status)
				if empty {
					var partition bnPartition
					require.NoError(t, f.db.Where("entity = ?", f.entity).Take(&partition).Error)
					require.Zero(t, partition.Count)
					require.False(t, partition.BnMin.Valid)
					require.False(t, partition.BnMax.Valid)
				} else {
					require.EqualValues(t, 2, integrationCount(t, f.db, f.target))
				}
			})
		}
	}
}

func TestMySQLMigrationSurvivesPruning(t *testing.T) {
	for _, kind := range []string{"contract", "topic"} {
		t.Run(kind, func(t *testing.T) {
			f := newIntegrationLogMigration(t, kind, 4)
			require.NoError(t, f.migrate())
			var tabler interface{ TableName() string }
			if kind == "contract" {
				tabler = f.ms.bcls.contractTabler(1)
			} else {
				tabler = f.ms.btls.topicTabler(1)
			}
			partitions := newBnPartitionedStore(f.db)
			_, err := partitions.growPartition(f.entity, tabler)
			require.NoError(t, err)
			pruned, err := partitions.pruneArchivePartitions(f.entity, tabler, 0)
			require.NoError(t, err)
			require.Len(t, pruned, 1)
			require.NoError(t, f.ms.Popn(1))
			done, err := partitions.hasPartition(f.entity)
			require.NoError(t, err)
			require.True(t, done)
			require.NoError(t, f.ms.Push(integrationData(1, 4)))
			require.EqualValues(t, 2, integrationCount(t, f.db, f.entity+"_1"))
			var sourceCount int64
			require.NoError(t, f.db.Table(f.source).Where(f.column+" = ?", 1).Count(&sourceCount).Error)
			require.Zero(t, sourceCount)
		})
	}
}

type integrationLogMigration struct {
	ms                     *MysqlStore[*store.EthData]
	db                     *gorm.DB
	model, entity          string
	source, column, target string
	migrate                func() error
}

func newIntegrationLogMigration(t *testing.T, kind string, count int) *integrationLogMigration {
	t.Helper()
	ms, db := newIntegrationStore(t, false)
	require.NoError(t, ms.Push(integrationData(1, count)))
	f := &integrationLogMigration{ms: ms, db: db}
	if kind == "contract" {
		var contract Contract
		require.NoError(t, db.First(&contract, 1).Error)
		f.model, f.entity, f.column = "contracts", "clogs_1", "cid"
		f.source = ms.ails.GetPartitionedTableName(contract.Address)
		f.migrate = func() error { return ms.bcls.migrate(&contract) }
	} else {
		var topic Topic
		require.NoError(t, db.First(&topic, 1).Error)
		f.model, f.entity, f.column = "topics", "tlogs_1", "tid"
		f.source = ms.tils.GetPartitionedTableName(topic.Hash)
		f.migrate = func() error { return ms.btls.migrate(&topic) }
	}
	f.target = f.entity + "_0"
	return f
}
