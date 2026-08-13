package mysql

import (
	"context"
	"sort"

	"github.com/Conflux-Chain/confura/store"
)

// migrationLogSource provides the storage operations required by the
// optimistic migration read protocol. It does not own retry or page semantics.
type migrationLogSource interface {
	isMigrationCompleted(id uint64) (bool, error)
	querySharedPartition(
		ctx context.Context, id uint64, value string, filter store.LogFilter,
	) ([]*store.Log, error)
	queryDedicatedPartitions(
		ctx context.Context, id uint64, value string, filter store.LogFilter,
	) ([]*store.Log, error)
}

type contractMigrationLogSource[T store.ChainData] struct {
	store *MysqlStore[T]
}

func (source contractMigrationLogSource[T]) isMigrationCompleted(id uint64) (bool, error) {
	return source.store.bcls.IsMigrationCompleted(id)
}

func (source contractMigrationLogSource[T]) querySharedPartition(
	ctx context.Context, id uint64, address string, filter store.LogFilter,
) ([]*store.Log, error) {
	return source.store.ails.GetAddressIndexedLogs(ctx, id, address, filter)
}

func (source contractMigrationLogSource[T]) queryDedicatedPartitions(
	ctx context.Context, id uint64, _ string, filter store.LogFilter,
) ([]*store.Log, error) {
	return source.store.bcls.GetContractLogs(ctx, id, filter)
}

type topicMigrationLogSource[T store.ChainData] struct {
	store *MysqlStore[T]
}

func (source topicMigrationLogSource[T]) isMigrationCompleted(id uint64) (bool, error) {
	return source.store.btls.IsMigrationCompleted(id)
}

func (source topicMigrationLogSource[T]) querySharedPartition(
	ctx context.Context, id uint64, topic string, filter store.LogFilter,
) ([]*store.Log, error) {
	return source.store.tils.GetTopicIndexedLogs(ctx, id, topic, filter)
}

func (source topicMigrationLogSource[T]) queryDedicatedPartitions(
	ctx context.Context, id uint64, topic string, filter store.LogFilter,
) ([]*store.Log, error) {
	return source.store.btls.GetTopicLogs(ctx, id, topic, filter)
}

// optimisticMigrationLogReader owns the protocol for reading logs that may be
// moved atomically from a shared partition to dedicated partitions.
type optimisticMigrationLogReader struct {
	source    migrationLogSource
	resolveID func(value string) (uint64, bool, error)
}

func (reader optimisticMigrationLogReader) read(
	ctx context.Context, values []string, filter store.LogFilter,
) ([]*store.Log, error) {
	var logs []*store.Log

	for _, value := range values {
		if err := checkGetLogsContext(ctx); err != nil {
			return nil, err
		}

		id, exists, err := reader.resolveID(value)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}

		perLogs, err := reader.readEach(ctx, id, value, filter)
		if err != nil {
			return nil, err
		}

		logs = append(logs, perLogs...)
		if store.IsBoundChecksEnabled(ctx) && len(logs) > int(store.MaxLogLimit) {
			return nil, newSuggestedFilterResultSetTooLargeError(&filter, logs, false)
		}
	}

	sort.Sort(store.LogSlice(logs))
	return logs, nil
}

func (reader optimisticMigrationLogReader) readEach(
	ctx context.Context, id uint64, value string, filter store.LogFilter,
) ([]*store.Log, error) {
	for {
		if err := checkGetLogsContext(ctx); err != nil {
			return nil, err
		}

		migrationCompleted, err := reader.source.isMigrationCompleted(id)
		if err != nil {
			return nil, err
		}

		if migrationCompleted {
			// A completed migration is authoritative and never moves back to the
			// shared partition, so this path needs no optimistic validation.
			return reader.source.queryDedicatedPartitions(ctx, id, value, filter)
		}

		logs, queryErr := reader.source.querySharedPartition(ctx, id, value, filter)

		// Validate the optimistic shared-partition read. Migration completion
		// invalidates this value, including a shared-query error.
		migrationCompleted, validationErr := reader.source.isMigrationCompleted(id)
		if validationErr != nil {
			return nil, validationErr
		}
		if migrationCompleted {
			continue
		}

		return logs, queryErr
	}
}

func checkGetLogsContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return store.ErrGetLogsTimeout
	default:
		return nil
	}
}
