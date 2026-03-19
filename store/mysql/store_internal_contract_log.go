package mysql

import (
	"context"
	"database/sql"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

// InternalContractLog stores virtual logs synthesized from internal contract traces.
type InternalContractLog struct {
	ID uint64 `gorm:"primaryKey;autoIncrement"`

	BlockNumber    uint64 `gorm:"column:bn;index:idx_addr_bn,priority:2;index:idx_addr_t0_bn,priority:3"`
	Epoch          uint64 `gorm:"index:idx_epoch;index:idx_addr_epoch,priority:2;index:idx_addr_t0_epoch,priority:3"`
	BlockHash      string `gorm:"column:bh;size:66;not null"`
	TxHash         string `gorm:"column:th;size:66;not null"`
	TxIndex        int    `gorm:"column:ti"`
	LogIndex       int    `gorm:"column:li"`
	TxLogIndex     int    `gorm:"column:tli"`
	AddressIndex   uint8  `gorm:"column:address;index:idx_addr_bn,priority:1;index:idx_addr_t0_bn,priority:1;index:idx_addr_epoch,priority:1;index:idx_addr_t0_epoch,priority:1"`
	Topic0Index    uint8  `gorm:"column:topic0;index:idx_addr_t0_bn,priority:2;index:idx_addr_t0_epoch,priority:2"`
	Topic1         string `gorm:"size:66"`
	Topic2         string `gorm:"size:66"`
	Topic3         string `gorm:"size:66"`
	Data           []byte `gorm:"type:mediumBlob"`
	BlockTimestamp uint64
}

func (InternalContractLog) TableName() string {
	return "internal_contract_logs"
}

// CfxInternalContractLogFilter defines the filter criteria for querying internal contract logs.
type CfxInternalContractLogFilter struct {
	*types.LogFilter

	ContractIndices   []uint8
	EventIndices      []uint8
	BlockHash2Numbers map[types.Hash]uint64
}

// InternalContractLogStore provides database operations for internal contract logs.
type InternalContractLogStore struct {
	*baseStore
}

func NewInternalContractLogStore(db *gorm.DB) *InternalContractLogStore {
	return &InternalContractLogStore{baseStore: newBaseStore(db)}
}

// Pop removes all logs at or after the given epoch (for reorg handling).
func (s *InternalContractLogStore) Pop(dbTx *gorm.DB, epochFrom uint64) error {
	return dbTx.Where("epoch >= ?", epochFrom).Delete(&InternalContractLog{}).Error
}

// GetLogs retrieves internal contract logs matching the given filter criteria within a repeatable-read transaction.
func (s *InternalContractLogStore) GetLogs(ctx context.Context, filter CfxInternalContractLogFilter) ([]InternalContractLog, error) {
	opts := &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	}

	var logs []InternalContractLog
	err := s.db.Transaction(func(tx *gorm.DB) error {
		syncInfo, err := s.getLatestSyncInfo(tx)
		if err != nil {
			return errors.WithMessage(err, "failed to get latest sync info")
		}

		query, err := s.buildRangeQuery(tx, filter, syncInfo)
		if err != nil {
			return errors.WithMessage(err, "failed to build range query")
		}

		query = s.applyIndexFilters(query, filter)
		query = s.applyTopicFilters(query, filter)

		if err := s.checkResultSetBounds(ctx, query, filter); err != nil {
			return err
		}

		return query.WithContext(ctx).
			Order("bn ASC").
			Limit(int(store.MaxLogLimit) + 1).
			Find(&logs).Error
	}, opts)

	return logs, err
}

// getLatestSyncInfo retrieves the most recent epoch-to-block mapping record.
func (s *InternalContractLogStore) getLatestSyncInfo(tx *gorm.DB) (CfxTraceSyncEpochBlockMap, error) {
	var info CfxTraceSyncEpochBlockMap
	if err := tx.Last(&info).Error; err != nil && !s.IsRecordNotFound(err) {
		return info, err
	}
	return info, nil
}

// buildRangeQuery constructs the WHERE clause for the primary range filter (block hashes, block range, or epoch range).
func (s *InternalContractLogStore) buildRangeQuery(
	query *gorm.DB, filter CfxInternalContractLogFilter, syncInfo CfxTraceSyncEpochBlockMap,
) (*gorm.DB, error) {
	switch {
	case len(filter.BlockHashes) > 0:
		return s.buildBlockHashQuery(query, filter, syncInfo)
	case filter.FromBlock != nil && filter.ToBlock != nil:
		return s.buildBlockRangeQuery(query, filter, syncInfo)
	case filter.FromEpoch != nil && filter.ToEpoch != nil:
		return s.buildEpochRangeQuery(query, filter, syncInfo)
	default:
		return nil, errors.New("invalid filter: no block hashes, block range or epoch range specified")
	}
}

func (s *InternalContractLogStore) buildBlockHashQuery(
	query *gorm.DB, filter CfxInternalContractLogFilter, syncInfo CfxTraceSyncEpochBlockMap,
) (*gorm.DB, error) {
	if len(filter.BlockHashes) != len(filter.BlockHash2Numbers) {
		return nil, errors.Errorf(
			"normalized block hash count mismatch, expected %d got %d",
			len(filter.BlockHashes), len(filter.BlockHash2Numbers),
		)
	}

	blockNumbers := make([]uint64, 0, len(filter.BlockHashes))
	for _, bh := range filter.BlockHashes {
		bn, ok := filter.BlockHash2Numbers[bh]
		if !ok {
			return nil, errors.Errorf("block hash not found: %v", bh)
		}
		if bn > syncInfo.BnMax {
			return nil, errors.Errorf("block number out of range: target %d > synced %d", bn, syncInfo.BnMax)
		}
		blockNumbers = append(blockNumbers, bn)
	}

	return query.Where("bn IN(?)", blockNumbers), nil
}

func (s *InternalContractLogStore) buildBlockRangeQuery(
	query *gorm.DB, filter CfxInternalContractLogFilter, syncInfo CfxTraceSyncEpochBlockMap,
) (*gorm.DB, error) {
	fromBlock := filter.FromBlock.ToInt().Uint64()
	toBlock := filter.ToBlock.ToInt().Uint64()

	if toBlock > syncInfo.BnMax {
		return nil, errors.Errorf(
			"block number out of range: target %d > synced %d", toBlock, syncInfo.BnMax,
		)
	}

	return query.Where("bn >= ? AND bn <= ?", fromBlock, toBlock), nil
}

func (s *InternalContractLogStore) buildEpochRangeQuery(
	query *gorm.DB, filter CfxInternalContractLogFilter, syncInfo CfxTraceSyncEpochBlockMap,
) (*gorm.DB, error) {
	epochFrom, ok := filter.FromEpoch.ToInt()
	if !ok {
		return nil, errors.Errorf("not a numeric from epoch: %v", filter.FromEpoch)
	}

	epochTo, ok := filter.ToEpoch.ToInt()
	if !ok {
		return nil, errors.Errorf("not a numeric to epoch: %v", filter.ToEpoch)
	}

	if epochTo.Uint64() > syncInfo.Epoch {
		return nil, errors.Errorf(
			"epoch out of range: target %d > synced %d", epochTo.Uint64(), syncInfo.Epoch,
		)
	}

	return query.Where("epoch >= ? AND epoch <= ?", epochFrom.Uint64(), epochTo.Uint64()), nil
}

// applyIndexFilters adds contract address and event index conditions.
func (s *InternalContractLogStore) applyIndexFilters(query *gorm.DB, filter CfxInternalContractLogFilter) *gorm.DB {
	if len(filter.ContractIndices) > 0 {
		query = query.Where("address IN(?)", filter.ContractIndices)
	}
	if len(filter.EventIndices) > 0 {
		query = query.Where("event IN(?)", filter.EventIndices)
	}
	return query
}

// applyTopicFilters converts log filter topics into variadic values and applies them.
func (s *InternalContractLogStore) applyTopicFilters(query *gorm.DB, filter CfxInternalContractLogFilter) *gorm.DB {
	vvs := make([]store.VariadicValue[types.Hash], 0, len(filter.Topics))
	for _, topics := range filter.Topics {
		vvs = append(vvs, store.NewVariadicValue(topics...))
	}

	return applyTopicsFilter(query, store.ToVariadicValuers(vvs...), &SecondaryOnlyTopicSchema)
}

// checkResultSetBounds validates the result set does not exceed MaxLogLimit.
// Returns a descriptive error with a suggested narrower range when possible.
func (s *InternalContractLogStore) checkResultSetBounds(
	ctx context.Context, query *gorm.DB, filter CfxInternalContractLogFilter,
) error {
	if !store.IsBoundChecksEnabled(ctx) {
		return nil
	}

	var exceeding struct{ Bn, Epoch uint64 }
	err := query.WithContext(ctx).
		Model(&InternalContractLog{}).
		Select("bn, epoch").
		Offset(int(store.MaxLogLimit)).
		Order("bn ASC").
		Take(&exceeding).Error

	if s.IsRecordNotFound(err) {
		return nil // within bounds
	}
	if err != nil {
		return errors.WithMessage(err, "failed to get first record exceeding limit")
	}

	return s.suggestNarrowedRange(filter, exceeding)
}

// suggestNarrowedRange builds a result set too large error with a suggested filter range.
func (s *InternalContractLogStore) suggestNarrowedRange(
	filter CfxInternalContractLogFilter, exceeding struct{ Bn, Epoch uint64 },
) error {
	if filter.FromBlock != nil {
		fromBn := filter.FromBlock.ToInt().Uint64()
		if exceeding.Bn > fromBn {
			r := store.NewSuggestedBlockRange(fromBn, exceeding.Bn-1, exceeding.Epoch)
			return store.NewSuggestedFilterResultSetTooLargeError(&r)
		}
	}

	if filter.FromEpoch != nil {
		if fromEpoch, ok := filter.FromEpoch.ToInt(); ok && exceeding.Epoch > fromEpoch.Uint64() {
			r := store.NewSuggestedEpochRange(fromEpoch.Uint64(), exceeding.Epoch-1)
			return store.NewSuggestedFilterResultSetTooLargeError(&r)
		}
	}

	return store.ErrFilterResultSetTooLarge
}
