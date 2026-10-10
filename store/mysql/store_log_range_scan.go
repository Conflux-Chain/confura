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

// Independent of the public RPC result limit.
const maxRangeScanCandidateLogs = 10_000

// tryRangeScanLogs distinguishes an empty successful result from a route miss.
func (ms *MysqlStore[T]) tryRangeScanLogs(ctx context.Context, filter store.LogFilter) ([]*store.Log, bool, error) {
	cfg := ms.config.GetLogsRangeScan
	if !cfg.Enabled {
		return nil, false, nil
	}
	addresses := rangeScanValues(filter.Contracts)
	if len(addresses) < cfg.MinAddresses || filter.BlockFrom > filter.BlockTo ||
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
	result, err := filterRangeScanLogs(ctx, candidates, addresses, filter.Topics)
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
	if err := checkGetLogsContext(ctx); err != nil {
		return nil, false, err
	}
	partitions, uncovered, err := ls.searchPartitions(ctx, bnPartitionedLogEntity,
		types.RangeUint64{From: filter.BlockFrom, To: filter.BlockTo})
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
		query := LogFilter{TableName: ls.getPartitionedTableName(&log{}, partition.Index), BlockFrom: filter.BlockFrom, BlockTo: filter.BlockTo}
		_, count, err := query.calculateQuerySetSize(ls.db.WithContext(ctx))
		if errors.Is(err, errInvalidLogIDRange) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if count > maxRangeScanCandidateLogs-estimated {
			return nil, false, nil
		}
		estimated += count
		queries = append(queries, query)
	}
	var candidates []*log
	for _, query := range queries {
		var rows []*log
		if err := query.read(ctx, ls.db, maxRangeScanCandidateLogs+1-len(candidates), &rows); err != nil {
			return nil, false, err
		}
		candidates = append(candidates, rows...)
		if len(candidates) > maxRangeScanCandidateLogs {
			return nil, false, nil
		}
	}
	return candidates, true, nil
}

// Both spaces persist canonical address strings in Extra.addr. Case folding
// handles eSpace checksum casing and core-space base32 casing alike.
func rangeScanValues(values store.VariadicValue[string]) map[string]bool {
	result := make(map[string]bool, values.Count())
	for _, value := range values.ToSlice() {
		result[strings.ToLower(value)] = true
	}
	return result
}

func filterRangeScanLogs(ctx context.Context, candidates []*log, addresses map[string]bool, topics []store.VariadicValue[string]) ([]*store.Log, error) {
	topicSets := make([]map[string]bool, len(topics))
	for i, topic := range topics {
		topicSets[i] = rangeScanValues(topic)
	}
	var result []*store.Log
	for _, row := range candidates {
		if err := checkGetLogsContext(ctx); err != nil {
			return nil, err
		}
		var extra struct {
			Address string `json:"addr"`
		}
		if err := json.Unmarshal(row.Extra, &extra); err != nil {
			return nil, errors.WithMessage(err, "failed to decode log address")
		}
		if !addresses[strings.ToLower(extra.Address)] {
			continue
		}
		values := [4]string{row.Topic0, row.Topic1, row.Topic2, row.Topic3}
		matches := true
		for i, set := range topicSets {
			if i >= len(values) {
				break
			}
			if len(set) > 0 && !set[strings.ToLower(values[i])] {
				matches = false
				break
			}
		}
		if matches {
			// Address-indexed readers do not expose physical row IDs.
			value := store.Log(*row)
			value.ID = 0
			result = append(result, &value)
		}
	}
	return result, nil
}
