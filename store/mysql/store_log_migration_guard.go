package mysql

import (
	"context"

	"github.com/Conflux-Chain/confura/store"
)

// migrationLogReadOperation provides the source-specific actions used by the
// optimistic migration read protocol. Each operation owns all query inputs so
// the guard remains independent of getLogs and scanLogs request semantics.
type migrationLogReadOperation interface {
	isMigrationCompleted() (bool, error)
	queryShared(ctx context.Context) ([]*store.Log, error)
	queryDedicated(ctx context.Context) ([]*store.Log, error)
}

// readMigrationAwareLogs reads from the authoritative side of an atomic log
// migration. A shared read is valid only if migration is still incomplete
// after that read finishes.
func readMigrationAwareLogs(
	ctx context.Context, operation migrationLogReadOperation,
) ([]*store.Log, error) {
	for {
		if err := checkGetLogsContext(ctx); err != nil {
			return nil, err
		}

		migrationCompleted, err := operation.isMigrationCompleted()
		if err != nil {
			return nil, err
		}

		if migrationCompleted {
			// A completed migration is authoritative and never moves back to the
			// shared partition, so this path needs no optimistic validation.
			return operation.queryDedicated(ctx)
		}

		logs, queryErr := operation.queryShared(ctx)

		// Migration completion invalidates the shared result, including a
		// shared-query error, so validate the state before returning either.
		migrationCompleted, validationErr := operation.isMigrationCompleted()
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

type contractGetLogsMigrationReadOperation[T store.ChainData] struct {
	store      *MysqlStore[T]
	contractID uint64
	contract   string
	filter     store.LogFilter
}

func (operation *contractGetLogsMigrationReadOperation[T]) isMigrationCompleted() (bool, error) {
	return operation.store.bcls.IsMigrationCompleted(operation.contractID)
}

func (operation *contractGetLogsMigrationReadOperation[T]) queryShared(
	ctx context.Context,
) ([]*store.Log, error) {
	return operation.store.ails.GetAddressIndexedLogs(
		ctx, operation.contractID, operation.contract, operation.filter,
	)
}

func (operation *contractGetLogsMigrationReadOperation[T]) queryDedicated(
	ctx context.Context,
) ([]*store.Log, error) {
	return operation.store.bcls.GetContractLogs(ctx, operation.contractID, operation.filter)
}

type topicGetLogsMigrationReadOperation[T store.ChainData] struct {
	store   *MysqlStore[T]
	topicID uint64
	topic   string
	filter  store.LogFilter
}

func (operation *topicGetLogsMigrationReadOperation[T]) isMigrationCompleted() (bool, error) {
	return operation.store.btls.IsMigrationCompleted(operation.topicID)
}

func (operation *topicGetLogsMigrationReadOperation[T]) queryShared(
	ctx context.Context,
) ([]*store.Log, error) {
	return operation.store.tils.GetTopicIndexedLogs(
		ctx, operation.topicID, operation.topic, operation.filter,
	)
}

func (operation *topicGetLogsMigrationReadOperation[T]) queryDedicated(
	ctx context.Context,
) ([]*store.Log, error) {
	return operation.store.btls.GetTopicLogs(
		ctx, operation.topicID, operation.topic, operation.filter,
	)
}

type contractScanLogsMigrationReadOperation[T store.ChainData] struct {
	store      *MysqlStore[T]
	contractID uint64
	contract   string
	topic0ID   *uint64
	params     store.ScanLogParams
}

func (operation *contractScanLogsMigrationReadOperation[T]) isMigrationCompleted() (bool, error) {
	return operation.store.bcls.IsMigrationCompleted(operation.contractID)
}

func (operation *contractScanLogsMigrationReadOperation[T]) queryShared(
	ctx context.Context,
) ([]*store.Log, error) {
	return operation.store.ails.ScanAddressIndexedLogs(
		ctx,
		operation.contractID,
		operation.contract,
		operation.topic0ID,
		operation.params,
	)
}

func (operation *contractScanLogsMigrationReadOperation[T]) queryDedicated(
	ctx context.Context,
) ([]*store.Log, error) {
	return operation.store.bcls.ScanContractLogs(
		ctx,
		operation.contractID,
		operation.topic0ID,
		operation.params,
	)
}

type topicScanLogsMigrationReadOperation[T store.ChainData] struct {
	store   *MysqlStore[T]
	topicID uint64
	topic0  string
	params  store.ScanLogParams
}

func (operation *topicScanLogsMigrationReadOperation[T]) isMigrationCompleted() (bool, error) {
	return operation.store.btls.IsMigrationCompleted(operation.topicID)
}

func (operation *topicScanLogsMigrationReadOperation[T]) queryShared(
	ctx context.Context,
) ([]*store.Log, error) {
	return operation.store.tils.ScanTopicIndexedLogs(
		ctx,
		operation.topicID,
		operation.topic0,
		operation.params,
	)
}

func (operation *topicScanLogsMigrationReadOperation[T]) queryDedicated(
	ctx context.Context,
) ([]*store.Log, error) {
	return operation.store.btls.ScanTopicLogs(
		ctx,
		operation.topicID,
		operation.topic0,
		operation.params,
	)
}
