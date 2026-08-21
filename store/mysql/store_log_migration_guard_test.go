package mysql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadMigrationAwareLogsRetriesInvalidatedSharedRead(t *testing.T) {
	sharedErr := errors.New("shared partition query failed")
	operation := &testMigrationLogReadOperation{
		migrateOnSecondCheck: true,
		sharedErr:            sharedErr,
		dedicatedLogs:        []*store.Log{{BlockNumber: 20}},
	}

	logs, err := readMigrationAwareLogs(context.Background(), operation)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, uint64(20), logs[0].BlockNumber)
	assert.Equal(t, 3, operation.stateChecks)
	assert.Equal(t, 1, operation.sharedQueries)
	assert.Equal(t, 1, operation.dedicatedQueries)
}

func TestReadMigrationAwareLogsReadsCompletedMigrationOnce(t *testing.T) {
	operation := &testMigrationLogReadOperation{
		completed:     true,
		dedicatedLogs: []*store.Log{{BlockNumber: 20}},
	}

	logs, err := readMigrationAwareLogs(context.Background(), operation)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, 1, operation.stateChecks)
	assert.Zero(t, operation.sharedQueries)
	assert.Equal(t, 1, operation.dedicatedQueries)
}

func TestReadMigrationAwareLogsReturnsStableSharedError(t *testing.T) {
	sharedErr := errors.New("shared partition query failed")
	operation := &testMigrationLogReadOperation{sharedErr: sharedErr}

	logs, err := readMigrationAwareLogs(context.Background(), operation)
	assert.Nil(t, logs)
	assert.ErrorIs(t, err, sharedErr)
	assert.Equal(t, 2, operation.stateChecks)
	assert.Equal(t, 1, operation.sharedQueries)
	assert.Zero(t, operation.dedicatedQueries)
}

func TestReadMigrationAwareLogsStopsWhenContextIsDone(t *testing.T) {
	operation := &testMigrationLogReadOperation{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	logs, err := readMigrationAwareLogs(ctx, operation)
	assert.Nil(t, logs)
	assert.ErrorIs(t, err, store.ErrGetLogsTimeout)
	assert.Zero(t, operation.stateChecks)
	assert.Zero(t, operation.sharedQueries)
	assert.Zero(t, operation.dedicatedQueries)
}

func TestGetContractLogsBuildsOperationsPerValueAndSorts(t *testing.T) {
	ms, db := newScanMysqlStoreTestHarness(t)
	createAddressScanTable(t, db)

	topicID, _, err := ms.ts.GetOrCreate("topic")
	require.NoError(t, err)
	firstID, _, err := ms.cs.AddContractIfAbsent("first")
	require.NoError(t, err)
	secondID, _, err := ms.cs.AddContractIfAbsent("second")
	require.NoError(t, err)

	tableName := ms.ails.GetPartitionedTableName("first")
	require.Equal(t, tableName, ms.ails.GetPartitionedTableName("second"))
	require.NoError(t, db.Table(tableName).Create([]*AddressIndexedLog{
		{ContractID: firstID, Topic0ID: topicID, BlockNumber: 30, Epoch: 30},
		{ContractID: secondID, Topic0ID: topicID, BlockNumber: 20, Epoch: 20},
	}).Error)

	ctx := store.NewContextWithBoundChecksDisabled(context.Background())
	logs, err := ms.getContractLogs(
		ctx,
		[]string{"first", "missing", "second"},
		store.LogFilter{BlockFrom: 0, BlockTo: 40},
	)
	require.NoError(t, err)
	assert.Equal(t, [][2]uint64{{20, 0}, {30, 0}}, storeLogKeys(logs))
}

func TestGetTopicLogsBuildsOperationsPerValueAndSorts(t *testing.T) {
	ms, db := newScanMysqlStoreTestHarness(t)
	createTopicScanTable(t, db)

	firstID, _, err := ms.ts.GetOrCreate("first")
	require.NoError(t, err)
	secondID, _, err := ms.ts.GetOrCreate("second")
	require.NoError(t, err)
	tableName := ms.tils.GetPartitionedTableName("first")
	require.Equal(t, tableName, ms.tils.GetPartitionedTableName("second"))
	require.NoError(t, db.Table(tableName).Create([]*TopicIndexedLog{
		{Topic0ID: firstID, BlockNumber: 30, Epoch: 30},
		{Topic0ID: secondID, BlockNumber: 20, Epoch: 20},
	}).Error)

	ctx := store.NewContextWithBoundChecksDisabled(context.Background())
	logs, err := ms.getTopicLogs(
		ctx,
		[]string{"first", "missing", "second"},
		store.LogFilter{BlockFrom: 0, BlockTo: 40},
	)
	require.NoError(t, err)
	assert.Equal(t, [][2]uint64{{20, 0}, {30, 0}}, storeLogKeys(logs))
}

func TestGetLogsOperationsUseDedicatedPartitionsAfterMigration(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		ms, db := newScanMysqlStoreTestHarness(t)
		contractID, _, err := ms.cs.AddContractIfAbsent("contract")
		require.NoError(t, err)
		topicID, _, err := ms.ts.GetOrCreate("topic")
		require.NoError(t, err)

		tableName := ms.bcls.getPartitionedTableName(ms.bcls.contractTabler(contractID), 0)
		createDedicatedContractScanTable(t, db, tableName)
		require.NoError(t, db.Create(&bnPartition{
			Entity: ms.bcls.contractEntity(contractID), Index: 0, Count: 1,
			BnMin: sql.NullInt64{Int64: 10, Valid: true},
			BnMax: sql.NullInt64{Int64: 10, Valid: true},
		}).Error)
		require.NoError(t, db.Table(tableName).Create(&contractLog{
			BlockNumber: 10, Epoch: 10, Topic0ID: topicID, LogIndex: 4,
		}).Error)

		ctx := store.NewContextWithBoundChecksDisabled(context.Background())
		logs, err := ms.getContractLogs(
			ctx, []string{"contract"}, store.LogFilter{BlockFrom: 10, BlockTo: 10},
		)
		require.NoError(t, err)
		require.Len(t, logs, 1)
		assert.Equal(t, "topic", logs[0].Topic0)
		assert.Equal(t, uint64(4), logs[0].LogIndex)
	})

	t.Run("topic", func(t *testing.T) {
		ms, db := newScanMysqlStoreTestHarness(t)
		topicID, _, err := ms.ts.GetOrCreate("topic")
		require.NoError(t, err)

		tableName := ms.btls.getPartitionedTableName(ms.btls.topicTabler(topicID), 0)
		createDedicatedTopicScanTable(t, db, tableName)
		require.NoError(t, db.Create(&bnPartition{
			Entity: ms.btls.topicEntity(topicID), Index: 0, Count: 1,
			BnMin: sql.NullInt64{Int64: 10, Valid: true},
			BnMax: sql.NullInt64{Int64: 10, Valid: true},
		}).Error)
		require.NoError(t, db.Table(tableName).Create(&topicLog{
			BlockNumber: 10, Epoch: 10, LogIndex: 5,
		}).Error)

		ctx := store.NewContextWithBoundChecksDisabled(context.Background())
		logs, err := ms.getTopicLogs(
			ctx, []string{"topic"}, store.LogFilter{BlockFrom: 10, BlockTo: 10},
		)
		require.NoError(t, err)
		require.Len(t, logs, 1)
		assert.Equal(t, "topic", logs[0].Topic0)
		assert.Equal(t, uint64(5), logs[0].LogIndex)
	})
}

type testMigrationLogReadOperation struct {
	completed            bool
	migrateOnSecondCheck bool
	stateChecks          int
	sharedQueries        int
	dedicatedQueries     int
	sharedErr            error
	dedicatedLogs        []*store.Log
}

func (operation *testMigrationLogReadOperation) isMigrationCompleted() (bool, error) {
	operation.stateChecks++
	if operation.migrateOnSecondCheck && operation.stateChecks == 2 {
		operation.completed = true
	}
	return operation.completed, nil
}

func (operation *testMigrationLogReadOperation) queryShared(
	context.Context,
) ([]*store.Log, error) {
	operation.sharedQueries++
	return nil, operation.sharedErr
}

func (operation *testMigrationLogReadOperation) queryDedicated(
	context.Context,
) ([]*store.Log, error) {
	operation.dedicatedQueries++
	return operation.dedicatedLogs, nil
}
