package mysql

import (
	"time"

	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type epochStatsType uint8

const (
	epochStatsEpochRange epochStatsType = iota + 1
	epochStatsEpochTotal
	epochStatsLogsPartEpochRange
)

// epoch statistics
type epochStats struct {
	ID uint32
	// key name
	Key string `gorm:"index:uidx_key_type,unique;size:66;not null"`
	// stats type
	Type epochStatsType `gorm:"index:uidx_key_type,unique;not null"`

	// min epoch for epoch range or total epoch number
	Epoch1 uint64
	// max epoch for epoch range or reversed for other use
	Epoch2 uint64

	CreatedAt time.Time
	UpdatedAt time.Time
}

// TableName overrides the table name used by epochStats to `epoch_stats`
func (epochStats) TableName() string {
	return "epoch_stats"
}

type EpochStatStore struct {
	*baseStore
}

func NewEpochStatStore(db *gorm.DB) *EpochStatStore {
	return &EpochStatStore{
		baseStore: newBaseStore(db),
	}
}

func (ess *EpochStatStore) loadEpochStats(est epochStatsType, keys ...string) ([]epochStats, error) {
	var result []epochStats

	db := ess.db.Where("type = ?", est)
	if len(keys) > 0 {
		db = db.Where("key = ?", keys)
	}

	if err := db.Find(&result).Error; err != nil {
		return nil, err
	}

	return result, nil
}

func (*EpochStatStore) initOrUpdateEpochRangeStats(dbTx *gorm.DB, dt store.EpochDataType, epochRange types.EpochRange) error {
	estats := epochStats{
		Key:    getEpochRangeStatsKey(dt),
		Type:   epochStatsEpochRange,
		Epoch1: epochRange.EpochFrom,
		Epoch2: epochRange.EpochTo,
	}

	return dbTx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}, {Name: "type"}},
		DoUpdates: clause.AssignmentColumns([]string{"epoch1", "epoch2"}),
	}).Create(&estats).Error
}

func (*EpochStatStore) initOrUpdateEpochTotalsStats(dbTx *gorm.DB, dt store.EpochDataType, totals uint64) error {
	estats := epochStats{
		Key:    getEpochTotalStatsKey(dt),
		Type:   epochStatsEpochTotal,
		Epoch1: totals,
	}

	return dbTx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}, {Name: "type"}},
		DoUpdates: clause.AssignmentColumns([]string{"epoch1"}),
	}).Create(&estats).Error
}

func (*EpochStatStore) initOrUpdateLogsPartitionEpochRangeStats(dbTx *gorm.DB, partition string, epochRange types.EpochRange) error {
	estats := epochStats{
		Key:    partition,
		Type:   epochStatsLogsPartEpochRange,
		Epoch1: epochRange.EpochFrom,
		Epoch2: epochRange.EpochTo,
	}

	return dbTx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}, {Name: "type"}},
		DoUpdates: clause.AssignmentColumns([]string{"epoch1", "epoch2"}),
	}).Create(&estats).Error
}

func (ess *EpochStatStore) getEntityCount(dt store.EpochDataType) (uint64, error) {
	var stat epochStats

	if _, err := ess.exists(&stat, "key = ? AND type = ?", getEpochTotalStatsKey(dt), epochStatsEpochTotal); err != nil {
		return 0, err
	}

	return stat.Epoch1, nil
}

func (ess *EpochStatStore) GetNumBlocks() (uint64, error) {
	return ess.getEntityCount(store.EpochBlock)
}

func (ess *EpochStatStore) GetNumTransactions() (uint64, error) {
	return ess.getEntityCount(store.EpochTransaction)
}

func (ess *EpochStatStore) GetNumLogs() (uint64, error) {
	return ess.getEntityCount(store.EpochLog)
}

func (*EpochStatStore) updateEntityCount(dbTx *gorm.DB, dt store.EpochDataType, delta int64) error {
	if delta == 0 {
		return nil
	}

	db := dbTx.Model(epochStats{}).Where("key = ? AND type = ?", getEpochTotalStatsKey(dt), epochStatsEpochTotal)

	if delta > 0 {
		return db.UpdateColumn("epoch1", gorm.Expr("epoch1 + ?", delta)).Error
	}

	return db.UpdateColumn("epoch1", gorm.Expr("GREATEST(0, CAST(epoch1 AS SIGNED) - ?)", -delta)).Error
}
