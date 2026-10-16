package mysql

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type bigLogMigrationTestHarness struct {
	db          *gorm.DB
	partitionID uint64
	entity      string
	sourceTable string
	targetTable string
	migrate     func() error
}

func TestBigLogMigrationRangeValidation(t *testing.T) {
	kinds := []struct {
		name    string
		factory func(*testing.T, *epochBlockMap, []uint64) *bigLogMigrationTestHarness
	}{
		{name: "contract", factory: newBigContractMigrationTestHarness},
		{name: "topic", factory: newBigTopicMigrationTestHarness},
	}

	tests := []struct {
		name          string
		mapping       *epochBlockMap
		sourceBlocks  []uint64
		wantErr       string
		wantSource    int64
		wantTarget    int64
		wantBnMin     sql.NullInt64
		wantBnMax     sql.NullInt64
		wantPartCount uint32
	}{
		{
			name:         "missing mapping fails closed",
			sourceBlocks: []uint64{150},
			wantErr:      "earliest block mapping not found",
			wantSource:   1,
		},
		{
			name:    "empty source fails closed",
			mapping: testEpochBlockMapping(10, 100, 109),
			wantErr: "indexed logs found for migration",
		},
		{
			name:         "inverted range rolls back",
			mapping:      testEpochBlockMapping(20, 200, 209),
			sourceBlocks: []uint64{150},
			wantErr:      "first covered block 200 exceeds last indexed block 150",
			wantSource:   1,
		},
		{
			name:          "valid range migrates",
			mapping:       testEpochBlockMapping(10, 100, 109),
			sourceBlocks:  []uint64{150, 160},
			wantSource:    0,
			wantTarget:    2,
			wantBnMin:     sql.NullInt64{Int64: 100, Valid: true},
			wantBnMax:     sql.NullInt64{Int64: 160, Valid: true},
			wantPartCount: 2,
		},
	}

	for _, kind := range kinds {
		kind := kind
		t.Run(kind.name, func(t *testing.T) {
			for _, test := range tests {
				test := test
				t.Run(test.name, func(t *testing.T) {
					harness := kind.factory(t, test.mapping, test.sourceBlocks)
					err := harness.migrate()
					if test.wantErr != "" {
						require.Error(t, err)
						assert.ErrorContains(t, err, test.wantErr)
					} else {
						require.NoError(t, err)
					}

					assertMigrationState(t, harness, test.wantSource, test.wantTarget,
						test.wantBnMin, test.wantBnMax, test.wantPartCount)
				})
			}
		})
	}
}

func newBigContractMigrationTestHarness(
	t *testing.T, mapping *epochBlockMap, sourceBlocks []uint64,
) *bigLogMigrationTestHarness {
	t.Helper()

	db := newBigLogMigrationTestDB(t)
	mapStore := &epochBlockMapStore[store.EpochData]{baseStore: newBaseStore(db)}
	addressStore := NewAddressIndexedLogStore[store.EpochData](db, nil, nil, 1)
	contractStore := &bigContractLogStore[store.EpochData]{
		bnPartitionedStore: newBnPartitionedStore(db),
		ebms:               mapStore,
		ails:               addressStore,
	}

	contract := &Contract{ID: 1, Address: "0x1"}
	partition := createBigLogMigrationTables(
		t, db, contractStore.contractEntity(contract.ID),
		&AddressIndexedLog{}, contractStore.contractTabler(contract.ID),
	)
	if mapping != nil {
		require.NoError(t, db.Create(mapping).Error)
	}

	sourceTable := addressStore.GetPartitionedTableName(contract.Address)
	for _, block := range sourceBlocks {
		require.NoError(t, db.Table(sourceTable).Create(&AddressIndexedLog{
			ContractID: contract.ID, BlockNumber: block,
		}).Error)
	}

	targetTable := contractStore.getPartitionedTableName(
		contractStore.contractTabler(contract.ID), partition.Index,
	)
	return &bigLogMigrationTestHarness{
		db: db, partitionID: partition.ID, entity: partition.Entity,
		sourceTable: sourceTable, targetTable: targetTable,
		migrate: func() error { return contractStore.migrate(contract, partition) },
	}
}

func newBigTopicMigrationTestHarness(
	t *testing.T, mapping *epochBlockMap, sourceBlocks []uint64,
) *bigLogMigrationTestHarness {
	t.Helper()

	db := newBigLogMigrationTestDB(t)
	mapStore := &epochBlockMapStore[store.EpochData]{baseStore: newBaseStore(db)}
	topicIndexStore := NewTopicIndexedLogStore[store.EpochData](db, nil, 1)
	topicStore := &bigTopicLogStore[store.EpochData]{
		bnPartitionedStore: newBnPartitionedStore(db),
		ebms:               mapStore,
		tils:               topicIndexStore,
	}

	topic := &Topic{ID: 1, Hash: "0x1"}
	partition := createBigLogMigrationTables(
		t, db, topicStore.topicEntity(topic.ID),
		&TopicIndexedLog{}, topicStore.topicTabler(topic.ID),
	)
	if mapping != nil {
		require.NoError(t, db.Create(mapping).Error)
	}

	sourceTable := topicIndexStore.GetPartitionedTableName(topic.Hash)
	for _, block := range sourceBlocks {
		require.NoError(t, db.Table(sourceTable).Create(&TopicIndexedLog{
			Topic0ID: topic.ID, BlockNumber: block,
		}).Error)
	}

	targetTable := topicStore.getPartitionedTableName(topicStore.topicTabler(topic.ID), partition.Index)
	return &bigLogMigrationTestHarness{
		db: db, partitionID: partition.ID, entity: partition.Entity,
		sourceTable: sourceTable, targetTable: targetTable,
		migrate: func() error { return topicStore.migrate(topic, partition) },
	}
}

func newBigLogMigrationTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := filepath.Join(t.TempDir(), "migration.db") + "?_journal_mode=WAL&_busy_timeout=5000"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&epochBlockMap{}, &bnPartition{}))
	return db
}

func createBigLogMigrationTables(
	t *testing.T,
	db *gorm.DB,
	entity string,
	sourceModel, targetModel interface{ TableName() string },
) bnPartition {
	t.Helper()

	partitionStore := newBnPartitionedStore(db)
	_, err := partitionStore.createPartitionedTable(db, sourceModel, 0)
	require.NoError(t, err)
	_, err = partitionStore.createPartitionedTable(db, targetModel, 0)
	require.NoError(t, err)

	partition := bnPartition{Entity: entity, Index: 0}
	require.NoError(t, db.Create(&partition).Error)
	return partition
}

func assertMigrationState(
	t *testing.T,
	harness *bigLogMigrationTestHarness,
	wantSource, wantTarget int64,
	wantBnMin, wantBnMax sql.NullInt64,
	wantPartCount uint32,
) {
	t.Helper()

	var sourceCount, targetCount int64
	require.NoError(t, harness.db.Table(harness.sourceTable).Count(&sourceCount).Error)
	require.NoError(t, harness.db.Table(harness.targetTable).Count(&targetCount).Error)
	assert.Equal(t, wantSource, sourceCount)
	assert.Equal(t, wantTarget, targetCount)

	var partition bnPartition
	require.NoError(t, harness.db.First(&partition, harness.partitionID).Error)
	assert.Equal(t, harness.entity, partition.Entity)
	assert.Equal(t, wantBnMin, partition.BnMin)
	assert.Equal(t, wantBnMax, partition.BnMax)
	assert.Equal(t, wantPartCount, partition.Count)
}

func testEpochBlockMapping(epoch, bnMin, bnMax uint64) *epochBlockMap {
	return &epochBlockMap{Epoch: epoch, BnMin: bnMin, BnMax: bnMax, PivotHash: "pivot"}
}
