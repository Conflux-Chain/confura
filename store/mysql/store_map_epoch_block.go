package mysql

import (
	"database/sql"

	"github.com/Conflux-Chain/confura/store"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

const (
	// epoch to block mapping partition size
	epochToBlockMappingPartitionSize = 50_000_000
)

// epochBlockMap mapping data from epoch to relative block info (such as block range and pivot block hash).
// Use MySQL ranged partitions for scalability and performance.
type epochBlockMap struct {
	// epoch number
	Epoch uint64 `gorm:"primaryKey;autoIncrement:false"`
	// min block number
	BnMin uint64 `gorm:"not null"`
	// max block number
	BnMax uint64 `gorm:"not null"`
	// pivot block hash used for parent hash checking
	PivotHash string `gorm:"size:66;not null"`
}

func (epochBlockMap) TableName() string {
	return "epoch_block_map"
}

// epochBlockMapStore used to get epoch to block map data.
type epochBlockMapStore struct {
	*baseStore

	// help partitioner
	partitioner *mysqlRangePartitioner
}

func newEpochBlockMapStore(db *gorm.DB, config *Config) *epochBlockMapStore {
	return &epochBlockMapStore{
		baseStore: newBaseStore(db),
		partitioner: newMysqlRangePartitioner(
			config.Database, epochBlockMap{}.TableName(), "epoch",
		),
	}
}

// preparePartition creates new table partition if necessary.
func (ebms *epochBlockMapStore) preparePartition(dataSlice []*store.EpochData) error {
	var latestPartitionIndex int

	partition, err := ebms.partitioner.latestPartition(ebms.db)
	if err != nil {
		return errors.WithMessage(err, "failed to get latest partition")
	}

	if partition != nil {
		latestPartitionIndex = ebms.partitioner.indexOfPartition(partition)
	} else {
		// create initial partition
		initPartitionIndex := int(dataSlice[0].Number / epochToBlockMappingPartitionSize)
		threshold := uint64(initPartitionIndex+1) * epochToBlockMappingPartitionSize

		err = ebms.partitioner.convert(ebms.db, initPartitionIndex, threshold)
		if err != nil {
			return errors.WithMessage(err, "failed to init range partitioned table")
		}

		latestPartitionIndex = int(initPartitionIndex)
	}

	for _, data := range dataSlice {
		if data.Number%epochToBlockMappingPartitionSize != 0 {
			continue
		}

		// create new partition if necessary
		partitionIndex := int(data.Number / epochToBlockMappingPartitionSize)
		if int(partitionIndex) <= latestPartitionIndex { // partition already exists
			continue
		}

		threshold := uint64(partitionIndex+1) * epochToBlockMappingPartitionSize
		err := ebms.partitioner.addPartition(ebms.db, partitionIndex, threshold)
		if err != nil {
			return errors.WithMessage(err, "failed to add partition")
		}

		latestPartitionIndex = partitionIndex
	}

	return nil
}

// MaxEpoch returns the max epoch within the map store.
func (e2bms *epochBlockMapStore) MaxEpoch() (uint64, bool, error) {
	var maxEpoch sql.NullInt64

	db := e2bms.db.Model(&epochBlockMap{}).Select("MAX(epoch)")
	if err := db.Find(&maxEpoch).Error; err != nil {
		return 0, false, err
	}

	if !maxEpoch.Valid {
		return 0, false, nil
	}

	return uint64(maxEpoch.Int64), true, nil
}

// findOneBlockMapping retrieves a single `epochBlockMap` record based on a condition and optional ordering.
func (e2bms *epochBlockMapStore) findOneBlockMapping(
	condition string, order string, args ...interface{}) (res epochBlockMap, existed bool, err error) {

	query := e2bms.db.Where(condition, args...)
	if order != "" {
		query = query.Order(order)
	}
	err = query.Take(&res).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return res, false, nil
	}
	if err != nil {
		return res, false, err
	}
	return res, true, nil
}

// FloorBlockMapping finds the `epochBlockMap` for the largest epoch ≤ the given epoch.
func (e2bms *epochBlockMapStore) FloorBlockMapping(epoch uint64) (epochBlockMap, bool, error) {
	return e2bms.findOneBlockMapping("epoch <= ?", "epoch DESC", epoch)
}

// CeilBlockMapping finds the `epochBlockMap` for the smallest epoch ≥ the given epoch.
func (e2bms *epochBlockMapStore) CeilBlockMapping(epoch uint64) (epochBlockMap, bool, error) {
	return e2bms.findOneBlockMapping("epoch >= ?", "epoch ASC", epoch)
}

// BlockMapping retrieves the `epochBlockMap` for the exact given epoch.
func (e2bms *epochBlockMapStore) BlockMapping(epoch uint64) (epochBlockMap, bool, error) {
	return e2bms.findOneBlockMapping("epoch = ?", "", epoch)
}

// LatestEpochBeforeBlock finds the latest epoch ≤ maxEpochNumber where BnMax ≤ blockNumber.
func (e2bms *epochBlockMapStore) LatestEpochBeforeBlock(maxEpochNumber, blockNumber uint64) (uint64, bool, error) {
	res, existed, err := e2bms.findOneBlockMapping("epoch <= ? AND bn_max <= ?", "epoch DESC", maxEpochNumber, blockNumber)
	if err != nil || !existed {
		return 0, false, err
	}
	return res.Epoch, true, nil
}

// PivotHash returns the pivot hash of the given epoch.
func (e2bms *epochBlockMapStore) PivotHash(epoch uint64) (string, bool, error) {
	var e2bmap epochBlockMap

	existed, err := e2bms.exists(&e2bmap, "epoch = ?", epoch)
	if err != nil {
		return "", false, err
	}

	return e2bmap.PivotHash, existed, nil
}

// Add batch saves epoch to block mapping data to db store.
func (e2bms *epochBlockMapStore) Add(dbTx *gorm.DB, dataSlice []*store.EpochData) error {
	var mappings []*epochBlockMap

	for _, data := range dataSlice {
		if len(data.Blocks) == 0 {
			continue
		}

		var pivotHash string
		if data.Hash != nil {
			pivotHash = string(*data.Hash)
		}

		firstBlock, endBlock := data.Blocks[0], data.Blocks[len(data.Blocks)-1]
		mappings = append(mappings, &epochBlockMap{
			Epoch:     data.Number,
			BnMin:     firstBlock.BlockNumber.ToInt().Uint64(),
			BnMax:     endBlock.BlockNumber.ToInt().Uint64(),
			PivotHash: pivotHash,
		})
	}

	if len(mappings) == 0 {
		return nil
	}

	return dbTx.Create(mappings).Error
}

// Remove remove epoch to block mappings of specific epoch range from db store.
func (e2bms *epochBlockMapStore) Remove(dbTx *gorm.DB, epochFrom, epochTo uint64) error {
	return dbTx.Where("epoch >= ? AND epoch <= ?", epochFrom, epochTo).Delete(&epochBlockMap{}).Error
}
