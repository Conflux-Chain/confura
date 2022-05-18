package mysql

import (
	"database/sql"
	"time"

	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// bnPartition partition by block number
type bnPartition struct {
	ID uint64

	// entity type
	Entity string `gorm:"index:uidx_entity_index,unique;priority:1;size:64;not null"`
	// shard index starting from 0
	Index uint32 `gorm:"index:uidx_entity_index,unique;priority:2;not null"`
	// num of rows on shard
	Count uint32 `gorm:"not null;default:0"`
	// min block number
	BnMin sql.NullInt64 `gorm:"default:null"`
	// max block number
	BnMax sql.NullInt64 `gorm:"default:null"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

// TableName overrides the table name
func (bnPartition) TableName() string {
	return "bn_partitions"
}

// bnPartitionedStore partitioned store by block number
type bnPartitionedStore struct {
	*baseStore
	partitionedStore
}

func newBnPartitionedStore(db *gorm.DB) *bnPartitionedStore {
	return &bnPartitionedStore{
		baseStore: newBaseStore(db),
	}
}

// growPartition appends a partition to the entity partition list. New table will be created with
// a new partition index, which will be consecutive to the max in the entity partition list.
func (bnps *bnPartitionedStore) growPartition(entity string, tabler schema.Tabler) (*bnPartition, error) {
	lastPart, existed, err := bnps.latestPartition(entity)
	if err != nil {
		return nil, err
	}

	newPart := bnPartition{Entity: entity}
	if existed {
		newPart.Index = lastPart.Index + 1
	}

	// No db transaction is needed here since the new partition table will be skipped
	// to be created next time. Besides, create new partition table within db transaction
	// will not rollback neither if failed.
	_, err = bnps.createPartitionedTable(bnps.db, tabler, newPart.Index)
	if err != nil {
		return nil, errors.WithMessagef(
			err, "failed to create partition table %v",
			bnps.getPartitionedTableName(tabler, newPart.Index),
		)
	}

	err = bnps.db.Create(&newPart).Error
	return &newPart, err
}

// shrinkPartition removes a partition with the smallest index from the entity paritition list.
// The partition table will also be dropped.
func (bnps *bnPartitionedStore) shrinkPartition(entity string, tabler schema.Tabler) (*bnPartition, error) {
	oldPart, existed, err := bnps.oldestPartition(entity)
	if err != nil || !existed {
		return nil, err
	}

	if err = bnps.db.Delete(oldPart).Error; err != nil {
		return nil, err
	}

	// No db transaction is needed here even if the old partition table failed to be dropped,
	// since it will never be used afterwards and we can drop the partition table manually.
	// Besides, drop old partition table within db transaction will not rollback neither if failed.
	_, err = bnps.deletePartitionedTable(bnps.db, tabler, oldPart.Index)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"entity": entity, "partition": oldPart,
			"table": bnps.getPartitionedTableName(tabler, oldPart.Index),
		}).WithError(err).Error("Failed to drop partition table")
	}

	return oldPart, nil
}

// searchPartitions searches for the partitions which hold the entity data covering the specified block range.
//
// If the search block range [sbn0, sbn1] overlaps with any area before the block range [bn0, bn1]
// covered by all entity partitions (sbn1 < bn0 or sbn0 < bn0 < sbn1), it will regard the entity data
// for the search block range as already pruned and raise an error.
//
// Otherwise, it will return the partitions (usally span at most two partitions) which hold the entity data
// for the search block range and the block range which is not covered by any entity partition.
func (bnps *bnPartitionedStore) searchPartitions(entity string, searchRange types.RangeUint64) (
	partitions []*bnPartition, uncoverings *types.RangeUint64, err error,
) {
	bnStart, bnEnd, err := bnps.bnRange(entity)
	if bnps.IsRecordNotFound(err) { // no partitions found
		return nil, &searchRange, nil
	}

	if err != nil {
		return nil, nil, err
	}

	if searchRange.To < bnStart {
		// search range is before the first partition
		return nil, nil, store.ErrAlreadyPruned
	}

	if searchRange.From > bnEnd {
		// search range is after the last partition
		return nil, &searchRange, nil
	}

	db := bnps.db.Where("bn_min <= ? AND bn_max >= ?", searchRange.To, searchRange.From)
	err = db.Find(&partitions).Error
	if err != nil {
		return nil, nil, err
	}

	if searchRange.From < bnStart && searchRange.To > bnStart {
		// search range intersects with the first partition
		return nil, nil, store.ErrAlreadyPruned
	}

	if searchRange.To > bnEnd && searchRange.From < bnEnd {
		// search range intersects with the last partition
		uncoverings = &types.RangeUint64{
			From: bnEnd + 1, To: searchRange.To,
		}
	}

	return
}

// deltaUpdateCount delta updates the accumulated data size for the latest entity partition.
// If the passed in `partitionIndex` parameter is non-negative, it will do sanity check to ensure the latest partition index
// is equal to the passed in `partitionIndex`.
func (bnps *bnPartitionedStore) deltaUpdateCount(dbTx *gorm.DB, entity string, partitionIndex, delta int) error {
	if delta == 0 {
		return nil
	}

	lastPart, existed, err := bnps.latestPartition(entity)
	if err != nil {
		return errors.WithMessage(err, "failed to get latest partition")
	}

	if !existed { // no availabe partition
		return store.ErrNotFound
	}

	if partitionIndex >= 0 && lastPart.Index != uint32(partitionIndex) { // sanity check on partition index
		return errors.WithMessagef(
			store.ErrNotFound,
			"expected partition index %v, got %v", partitionIndex, lastPart.Index,
		)
	}

	if delta > 0 {
		return dbTx.UpdateColumn("count", gorm.Expr("count + ?", delta)).Error
	}

	return dbTx.UpdateColumn("count", gorm.Expr("GREATEST(0, CAST(count AS SIGNED) - ?)", -delta)).Error
}

// expandBnRange expands block number range of the latest entity partition.
// If the passed in `partitionIndex` parameter is non-negative, it will do sanity check to ensure the latest partition index
// is equal to the passed in `partitionIndex`.
func (bnps *bnPartitionedStore) expandBnRange(dbTx *gorm.DB, entity string, partitionIndex int, from, to uint64) error {
	lastPart, existed, err := bnps.latestPartition(entity)
	if err != nil {
		return errors.WithMessage(err, "failed to get latest partition")
	}

	if !existed { // no availabe partition to expand
		return store.ErrNotFound
	}

	if partitionIndex >= 0 && lastPart.Index != uint32(partitionIndex) { // sanity check on partition index
		return errors.WithMessagef(
			store.ErrNotFound,
			"expected partition index %v, got %v", partitionIndex, lastPart.Index,
		)
	}

	updates := map[string]interface{}{
		"bn_min": gorm.Expr("IFNULL(bn_min, ?)", from),
		"bn_max": to,
	}

	return dbTx.Model(&bnPartition{}).Where(lastPart).Updates(updates).Error
}

// shrinkBnRange shrink block number range from the latest entity partition.
func (bnps *bnPartitionedStore) shrinkBnRange(dbTx *gorm.DB, entity string, to uint64) error {
	startIdx, endIdx, err := bnps.indexRange(entity)
	if err != nil {
		return errors.WithMessage(err, "failed to get index range")
	}

	// shrink from the latest partition in case the shrunk range is not fully covered by any partition
	for idx := int32(endIdx); idx >= int32(startIdx); idx-- {
		part, err := bnps.getPartitionByIndex(entity, uint32(idx))
		if err != nil {
			return errors.WithMessagef(err, "failed to get partition with index %v", idx)
		}

		if !part.BnMin.Valid || !part.BnMax.Valid {
			// no entity data on partition
			continue
		}

		if uint64(part.BnMax.Int64) <= to { // shrunk over
			break
		}

		updates := make(map[string]interface{})

		if uint64(part.BnMin.Int64) <= to {
			updates["bn_max"] = to
		} else { // shrunk through the whole partition
			updates["bn_max"] = gorm.Expr("NULL")
			updates["bn_min"] = gorm.Expr("NULL")
		}

		err = dbTx.Model(&bnPartition{}).Where(part).Updates(updates).Error
		if err != nil {
			return errors.WithMessagef(err, "failed to shrink partition with index %v", idx)
		}
	}

	return nil
}

// oldestPartition returns the oldest created partition (also with the min index) from
// the entity partition list.
func (bnps *bnPartitionedStore) oldestPartition(entity string) (*bnPartition, bool, error) {
	return bnps.latestOrOldestPartition(entity, false)
}

// latestPartition returns the latest created partition (also with the max index) from
// the entity partition list.
func (bnps *bnPartitionedStore) latestPartition(entity string) (*bnPartition, bool, error) {
	return bnps.latestOrOldestPartition(entity, true)
}

// latestOrOldestPartition returns the latest or oldest created partition (also with the max index) from
// the entity partition list.
func (bnps *bnPartitionedStore) latestOrOldestPartition(entity string, latest bool) (*bnPartition, bool, error) {
	var partition bnPartition

	db := bnps.db.Where("entity = ?", entity)
	if latest {
		db = db.Order("index desc")
	} else {
		db = db.Order("index asc")
	}

	err := db.First(&partition).Error
	if bnps.IsRecordNotFound(err) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}

	return &partition, true, nil
}

// getPartitionByIndex gets partition by index from entity partition list.
func (bnps *bnPartitionedStore) getPartitionByIndex(entity string, index uint32) (*bnPartition, error) {
	bnp := bnPartition{
		Entity: entity, Index: index,
	}

	res := bnps.db.Where(bnp).First(&bnp)
	return &bnp, res.Error
}

// indexRange returns the partition index range for the entity.
func (bnps *bnPartitionedStore) indexRange(entity string) (start uint32, end uint32, err error) {
	v0, v1, err := bnps.entityRange("MAX(index) AS max, MIN(index) AS min", entity)
	if err != nil {
		return
	}

	return uint32(v0), uint32(v1), nil
}

// bnRange returns the block number range covered by the entity partitions.
func (bnps *bnPartitionedStore) bnRange(entity string) (start uint64, end uint64, err error) {
	return bnps.entityRange("MAX(bn_max) AS max, MIN(bn_min) AS min", entity)
}

// range returns entity range covered by partitions.
func (bnps *bnPartitionedStore) entityRange(selector string, entity string) (start uint64, end uint64, err error) {
	var er struct {
		Max, Min sql.NullInt64
	}

	db := bnps.db.Select(selector).Model(&bnPartition{}).Where("entity = ?", entity)
	if err = db.Find(&er).Error; err != nil {
		return
	}

	if !er.Max.Valid || !er.Min.Valid {
		err = store.ErrNotFound
		return
	}

	start = uint64(er.Min.Int64)
	end = uint64(er.Max.Int64)
	return
}
