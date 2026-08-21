package mysql

import (
	"context"

	"github.com/Conflux-Chain/confura/store"
	"github.com/pkg/errors"
)

type scanPartitionQuery func(
	ctx context.Context,
	partition *bnPartition,
	blockFrom uint64,
	blockTo uint64,
	remaining int,
) ([]*store.Log, error)

func validateScanLogParams(params store.ScanLogParams) error {
	if params.Limit <= 0 {
		return errors.New("scan log limit must be positive")
	}

	if params.Filter.BlockFrom > params.Filter.BlockTo {
		return errors.Errorf(
			"scan log block range is inverted: from %v to %v",
			params.Filter.BlockFrom, params.Filter.BlockTo,
		)
	}

	return nil
}

// scanPartitions scans block-number partitions in the requested direction.
// searchPartitions returns ordered, non-overlapping metadata, so concatenating
// individually ordered results preserves the global scan order.
func scanPartitions(
	ctx context.Context,
	partitions []*bnPartition,
	params store.ScanLogParams,
	query scanPartitionQuery,
) ([]*store.Log, error) {
	result := make([]*store.Log, 0, params.Limit)

	for offset := 0; offset < len(partitions); offset++ {
		if err := checkGetLogsContext(ctx); err != nil {
			return nil, err
		}

		index := offset
		if params.Reverse {
			index = len(partitions) - 1 - offset
		}

		partition := partitions[index]
		if partition == nil || !partition.BnMin.Valid || !partition.BnMax.Valid ||
			partition.BnMin.Int64 < 0 || partition.BnMax.Int64 < 0 {
			return nil, errors.Errorf("invalid bn partition metadata at position %v", index)
		}

		partitionFrom := uint64(partition.BnMin.Int64)
		partitionTo := uint64(partition.BnMax.Int64)

		if params.Cursor != nil {
			if !params.Reverse && partitionTo < params.Cursor.BlockNumber {
				continue
			}
			if params.Reverse && partitionFrom > params.Cursor.BlockNumber {
				continue
			}
		}

		blockFrom := max(params.Filter.BlockFrom, partitionFrom)
		blockTo := min(params.Filter.BlockTo, partitionTo)

		// Tighten the range to the cursor block. The SQL cursor predicate still
		// handles exclusive log-index filtering within that block.
		if params.Cursor != nil {
			if params.Reverse {
				blockTo = min(blockTo, params.Cursor.BlockNumber)
			} else {
				blockFrom = max(blockFrom, params.Cursor.BlockNumber)
			}
		}

		if blockFrom > blockTo {
			continue
		}

		remaining := params.Limit - len(result)
		if remaining == 0 {
			break
		}

		logs, err := query(ctx, partition, blockFrom, blockTo, remaining)
		if err != nil {
			return nil, err
		}
		if len(logs) > remaining {
			return nil, errors.Errorf(
				"scan partition returned %v logs with remaining limit %v",
				len(logs), remaining,
			)
		}

		result = append(result, logs...)
		if len(result) == params.Limit {
			break
		}
	}

	return result, nil
}

// ScanLogs scans persisted logs using an exclusive keyset cursor.
func (ms *MysqlStore[T]) ScanLogs(
	ctx context.Context, params store.ScanLogParams,
) ([]*store.Log, error) {
	if err := validateScanLogParams(params); err != nil {
		return nil, err
	}

	if params.Filter.Contract != "" {
		return ms.scanContractLogs(ctx, params)
	}

	if params.Filter.Topic0 != "" {
		return ms.scanTopicLogs(ctx, params)
	}

	return ms.ls.ScanLogs(ctx, params)
}

func (ms *MysqlStore[T]) scanContractLogs(
	ctx context.Context, params store.ScanLogParams,
) ([]*store.Log, error) {
	contract := params.Filter.Contract
	cid, exists, err := ms.resolveContractLogID(contract)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}

	topic0ID, matched, err := ms.resolveOptionalScanTopicID(params.Filter.Topic0)
	if err != nil {
		return nil, err
	}
	if !matched {
		return nil, nil
	}

	operation := &contractScanLogsMigrationReadOperation[T]{
		store:      ms,
		contractID: cid,
		contract:   contract,
		topic0ID:   topic0ID,
		params:     params,
	}
	return readMigrationAwareLogs(ctx, operation)
}

func (ms *MysqlStore[T]) scanTopicLogs(
	ctx context.Context, params store.ScanLogParams,
) ([]*store.Log, error) {
	topic0 := params.Filter.Topic0
	tid, exists, err := ms.resolveTopicLogID(topic0)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}

	operation := &topicScanLogsMigrationReadOperation[T]{
		store:   ms,
		topicID: tid,
		topic0:  topic0,
		params:  params,
	}
	return readMigrationAwareLogs(ctx, operation)
}

func (ms *MysqlStore[T]) resolveOptionalScanTopicID(
	topic0 string,
) (*uint64, bool, error) {
	if topic0 == "" {
		return nil, true, nil
	}

	tid, exists, err := ms.resolveTopicLogID(topic0)
	if err != nil || !exists {
		return nil, exists, err
	}

	return &tid, true, nil
}
