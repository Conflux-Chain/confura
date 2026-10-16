package mysql

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/pkg/errors"
)

// maxRangeScanCandidateLogs is the maximum number of candidate logs to read for a range scan.
// If the estimated or actual number of logs exceeds this limit, the range scan will be aborted
// and routing will be left to the caller.
const maxRangeScanCandidateLogs = 10_000

// tryRangeScanLogs distinguishes an empty successful result from a route miss.
func (ms *MysqlStore[T]) tryRangeScanLogs(ctx context.Context, filter store.LogFilter) ([]*store.Log, bool, error) {
	cfg := ms.config.GetLogsRangeScan
	if !cfg.Enabled {
		return nil, false, nil
	}

	if filter.Contracts.Count() < cfg.MinAddresses ||
		(cfg.MaxBlocks > 0 && filter.BlockTo-filter.BlockFrom >= cfg.MaxBlocks) {
		return nil, false, nil
	}

	candidates, handled, err := ms.ls.getRangeScanCandidates(ctx, filter)
	if err != nil {
		if timeout := checkGetLogsContext(ctx); timeout != nil {
			err = timeout
		}
		return nil, false, err
	}

	if !handled {
		return nil, false, nil
	}

	result, err := filterRangeScanLogs(ctx, filter, candidates)
	if err != nil {
		return nil, false, err
	}

	sort.Sort(store.LogSlice(result))
	if store.IsBoundChecksEnabled(ctx) && len(result) > int(store.MaxLogLimit) {
		return nil, true, newSuggestedFilterResultSetTooLargeError(&filter, result, true)
	}

	return result, true, nil
}

// getRangeScanCandidates reads a fully covered range only when its estimated
// and actual row counts fit the candidate cap. A miss leaves routing to the caller.
func (ls *logStore[T]) getRangeScanCandidates(ctx context.Context, filter store.LogFilter) ([]*log, bool, error) {
	partitions, uncovered, err := ls.searchPartitions(
		ctx, bnPartitionedLogEntity, types.RangeUint64{
			From: filter.BlockFrom,
			To:   filter.BlockTo,
		},
	)
	if errors.Is(err, store.ErrAlreadyPruned) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if uncovered != nil {
		return nil, false, nil
	}

	queries := make([]LogFilter, 0, len(partitions))
	var estimated uint64
	for _, partition := range partitions {
		query := LogFilter{
			TableName: ls.getPartitionedTableName(&log{}, partition.Index),
			BlockFrom: filter.BlockFrom,
			BlockTo:   filter.BlockTo,
		}
		_, count, err := query.calculateQuerySetSize(ls.db.WithContext(ctx))
		if errors.Is(err, errInvalidLogIDRange) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}

		if count+estimated > maxRangeScanCandidateLogs {
			return nil, false, nil
		}

		estimated += count
		queries = append(queries, query)
	}

	var candidates []*log
	for _, query := range queries {
		var rows []*log
		limit := maxRangeScanCandidateLogs + 1 - len(candidates)
		if err := query.read(ctx, ls.db, limit, &rows); err != nil {
			return nil, false, err
		}

		candidates = append(candidates, rows...)
		if len(candidates) > maxRangeScanCandidateLogs {
			return nil, false, nil
		}
	}
	return candidates, true, nil
}

func filterRangeScanLogs(ctx context.Context, filter store.LogFilter, candidates []*log) ([]*store.Log, error) {
	addrSets := make(map[string]bool, filter.Contracts.Count())
	for _, addr := range filter.Contracts.ToSlice() {
		addrSets[strings.ToLower(addr)] = true
	}

	topicSets := make([]map[string]bool, len(filter.Topics))
	for i, topics := range filter.Topics {
		topicSets[i] = make(map[string]bool, topics.Count())
		for _, v := range topics.ToSlice() {
			topicSets[i][strings.ToLower(v)] = true
		}
	}

	var result []*store.Log
	for _, row := range candidates {
		if err := checkGetLogsContext(ctx); err != nil {
			return nil, err
		}

		matches := true
		topics := [4]string{
			row.Topic0, row.Topic1, row.Topic2, row.Topic3,
		}

		for i, set := range topicSets {
			if i >= len(topics) {
				break
			}

			if len(set) > 0 && !set[strings.ToLower(topics[i])] {
				matches = false
				break
			}
		}

		if !matches {
			continue
		}

		var extra struct {
			Address string `json:"addr"`
		}
		if err := json.Unmarshal(row.Extra, &extra); err != nil {
			return nil, errors.WithMessage(err, "failed to decode log address")
		}

		if !addrSets[strings.ToLower(extra.Address)] {
			continue
		}

		// Address-indexed readers do not expose physical row IDs.
		value := store.Log(*row)
		value.ID = 0
		result = append(result, &value)
	}
	return result, nil
}
