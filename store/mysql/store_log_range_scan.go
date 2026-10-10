package mysql

import (
	"context"
	"sort"
	"time"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util"
	metricUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
)

// Independent of the public RPC result limit.
const maxRangeScanCandidateLogs = 10_000

// tryRangeScanLogs distinguishes an empty successful result from a route miss.
func (ms *MysqlStore[T]) tryRangeScanLogs(ctx context.Context, filter store.LogFilter) (result []*store.Log, handled bool, err error) {
	var chain T
	switch any(chain).(type) {
	case store.EthData, *store.EthData:
	default:
		return nil, false, nil
	}
	cfg := ms.config.GetLogsRangeScan
	if !cfg.Enabled {
		return nil, false, nil
	}
	reason := "addresses"
	defer func() {
		if err != nil {
			reason = "error"
		}
		metricUtil.GetOrRegisterCounter("infura/store/ethGetLogs/rangeScan/outcome/%s", reason).Inc(1)
	}()
	addresses := make(map[common.Address]struct{})
	for _, addr := range filter.Contracts.ToSlice() {
		addresses[common.HexToAddress(addr)] = struct{}{}
	}
	if len(addresses) < cfg.MinAddresses {
		return nil, false, nil
	}
	reason = "blocks"
	if filter.BlockFrom > filter.BlockTo || (cfg.MaxBlocks > 0 && filter.BlockTo-filter.BlockFrom >= cfg.MaxBlocks) {
		return nil, false, nil
	}
	if err := checkGetLogsContext(ctx); err != nil {
		return nil, false, err
	}
	partitions, uncovered, err := ms.ls.searchPartitions(ctx, bnPartitionedLogEntity, types.RangeUint64{From: filter.BlockFrom, To: filter.BlockTo})
	reason = "coverage"
	if errors.Is(err, store.ErrAlreadyPruned) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, rangeScanError(ctx, err)
	}
	if uncovered != nil {
		return nil, false, nil
	}
	reason = "estimate"
	started := time.Now()
	var estimated uint64
	for _, partition := range partitions {
		query := LogFilter{TableName: ms.ls.getPartitionedTableName(&log{}, partition.Index), BlockFrom: filter.BlockFrom, BlockTo: filter.BlockTo}
		_, count, e := query.calculateQuerySetSize(ms.ls.db.WithContext(ctx))
		if errors.Is(e, errInvalidLogIDRange) {
			return nil, false, nil
		}
		if e != nil {
			return nil, false, rangeScanError(ctx, e)
		}
		if count > maxRangeScanCandidateLogs-estimated {
			return nil, false, nil
		}
		estimated += count
	}
	metricUtil.GetOrRegisterTimer("infura/store/ethGetLogs/rangeScan/estimate").UpdateSince(started)
	metricUtil.GetOrRegisterHistogram("infura/store/ethGetLogs/rangeScan/candidates").Update(int64(estimated))
	reason = "actualCount"
	var candidates []*log
	for _, partition := range partitions {
		query := LogFilter{TableName: ms.ls.getPartitionedTableName(&log{}, partition.Index), BlockFrom: filter.BlockFrom, BlockTo: filter.BlockTo}
		var rows []*log
		if e := query.read(ctx, ms.ls.db, maxRangeScanCandidateLogs+1-len(candidates), &rows); e != nil {
			return nil, false, rangeScanError(ctx, e)
		}
		candidates = append(candidates, rows...)
		if len(candidates) > maxRangeScanCandidateLogs {
			return nil, false, nil
		}
	}
	topics := make([][]common.Hash, len(filter.Topics))
	for i, topic := range filter.Topics {
		for _, value := range topic.ToSlice() {
			topics[i] = append(topics[i], common.HexToHash(value))
		}
	}
	for _, row := range candidates {
		if e := checkGetLogsContext(ctx); e != nil {
			return nil, false, e
		}
		original := (*store.Log)(row)
		parsed, e := original.ToEthLog()
		if e != nil {
			return nil, false, e
		}
		if _, ok := addresses[parsed.Address]; ok && util.MatchEthLogTopics(parsed, topics) {
			// Address-indexed readers do not expose physical row IDs.
			original.ID = 0
			result = append(result, original)
		}
	}
	sort.Sort(store.LogSlice(result))
	reason = "hit"
	if store.IsBoundChecksEnabled(ctx) && len(result) > int(store.MaxLogLimit) {
		return nil, true, newSuggestedFilterResultSetTooLargeError(&filter, result, true)
	}
	return result, true, nil
}

func rangeScanError(ctx context.Context, err error) error {
	if timeout := checkGetLogsContext(ctx); timeout != nil {
		return timeout
	}
	return err
}
