package mysql

import (
	"context"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type migrationAwareLogSource struct {
	migrationCompleted map[uint64]bool
	migratingID        uint64
	stateChecks        map[uint64]int
	sharedQueries      map[uint64]int
	dedicatedQueries   map[uint64]int
	sharedLogs         map[uint64][]*store.Log
	dedicatedLogs      map[uint64][]*store.Log
	sharedErr          error
}

func (source *migrationAwareLogSource) isMigrationCompleted(id uint64) (bool, error) {
	source.stateChecks[id]++
	if id == source.migratingID && source.stateChecks[id] == 2 {
		source.migrationCompleted[id] = true
	}
	return source.migrationCompleted[id], nil
}

func (source *migrationAwareLogSource) querySharedPartition(
	_ context.Context, id uint64, _ string, _ store.LogFilter,
) ([]*store.Log, error) {
	source.sharedQueries[id]++
	return source.sharedLogs[id], source.sharedErr
}

func (source *migrationAwareLogSource) queryDedicatedPartitions(
	_ context.Context, id uint64, _ string, _ store.LogFilter,
) ([]*store.Log, error) {
	source.dedicatedQueries[id]++
	return source.dedicatedLogs[id], nil
}

func TestOptimisticMigrationLogReaderRetriesOnlyInvalidatedValue(t *testing.T) {
	source := &migrationAwareLogSource{
		migrationCompleted: make(map[uint64]bool),
		migratingID:        2,
		stateChecks:        make(map[uint64]int),
		sharedQueries:      make(map[uint64]int),
		dedicatedQueries:   make(map[uint64]int),
		sharedLogs: map[uint64][]*store.Log{
			1: {{BlockNumber: 30}},
			// The shared partition is empty after the migration commits.
			2: nil,
		},
		dedicatedLogs: map[uint64][]*store.Log{
			2: {{BlockNumber: 20}},
		},
	}
	reader := optimisticMigrationLogReader{source: source, resolveID: resolveTestLogID}

	logs, err := reader.read(context.Background(), []string{"first", "migrating"}, store.LogFilter{})
	require.NoError(t, err)
	require.Len(t, logs, 2)
	assert.Equal(t, uint64(20), logs[0].BlockNumber)
	assert.Equal(t, uint64(30), logs[1].BlockNumber)

	// The stable first value is retained while only the value invalidated by
	// migration is retried against its dedicated partitions.
	assert.Equal(t, 1, source.sharedQueries[1])
	assert.Equal(t, 1, source.sharedQueries[2])
	assert.Equal(t, 1, source.dedicatedQueries[2])
}

func TestOptimisticMigrationLogReaderDoesNotRecheckCompletedMigration(t *testing.T) {
	source := &migrationAwareLogSource{
		migrationCompleted: map[uint64]bool{1: true},
		stateChecks:        make(map[uint64]int),
		sharedQueries:      make(map[uint64]int),
		dedicatedQueries:   make(map[uint64]int),
		dedicatedLogs:      map[uint64][]*store.Log{1: {{BlockNumber: 20}}},
	}
	reader := optimisticMigrationLogReader{source: source, resolveID: resolveTestLogID}

	logs, err := reader.read(context.Background(), []string{"migrated"}, store.LogFilter{})
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, 1, source.stateChecks[1])
	assert.Zero(t, source.sharedQueries[1])
	assert.Equal(t, 1, source.dedicatedQueries[1])
}

func TestOptimisticMigrationLogReaderDiscardsInvalidatedSharedQueryError(t *testing.T) {
	sharedErr := errors.New("shared partition query failed")
	source := &migrationAwareLogSource{
		migrationCompleted: make(map[uint64]bool),
		migratingID:        1,
		stateChecks:        make(map[uint64]int),
		sharedQueries:      make(map[uint64]int),
		dedicatedQueries:   make(map[uint64]int),
		dedicatedLogs:      map[uint64][]*store.Log{1: {{BlockNumber: 20}}},
		sharedErr:          sharedErr,
	}
	reader := optimisticMigrationLogReader{source: source, resolveID: resolveTestLogID}

	logs, err := reader.read(context.Background(), []string{"first"}, store.LogFilter{})
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, 1, source.sharedQueries[1])
	assert.Equal(t, 1, source.dedicatedQueries[1])
}

func TestOptimisticMigrationLogReaderReturnsStableSharedQueryError(t *testing.T) {
	sharedErr := errors.New("shared partition query failed")
	source := &migrationAwareLogSource{
		migrationCompleted: make(map[uint64]bool),
		stateChecks:        make(map[uint64]int),
		sharedQueries:      make(map[uint64]int),
		dedicatedQueries:   make(map[uint64]int),
		sharedErr:          sharedErr,
	}
	reader := optimisticMigrationLogReader{source: source, resolveID: resolveTestLogID}

	logs, err := reader.read(context.Background(), []string{"first"}, store.LogFilter{})
	assert.Nil(t, logs)
	assert.ErrorIs(t, err, sharedErr)
	assert.Equal(t, 2, source.stateChecks[1])
	assert.Equal(t, 1, source.sharedQueries[1])
	assert.Zero(t, source.dedicatedQueries[1])
}

func TestOptimisticMigrationLogReaderStopsWhenContextIsDone(t *testing.T) {
	source := &migrationAwareLogSource{
		migrationCompleted: make(map[uint64]bool),
		stateChecks:        make(map[uint64]int),
		sharedQueries:      make(map[uint64]int),
		dedicatedQueries:   make(map[uint64]int),
	}
	reader := optimisticMigrationLogReader{source: source, resolveID: resolveTestLogID}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	logs, err := reader.read(ctx, []string{"first"}, store.LogFilter{})
	assert.Nil(t, logs)
	assert.ErrorIs(t, err, store.ErrGetLogsTimeout)
	assert.Empty(t, source.stateChecks)
}

func resolveTestLogID(value string) (uint64, bool, error) {
	ids := map[string]uint64{"first": 1, "migrating": 2, "migrated": 1}
	id, exists := ids[value]
	return id, exists, nil
}
