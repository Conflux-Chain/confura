package mysql

import (
	"database/sql"

	"github.com/Conflux-Chain/confura/store"
	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

const (
	// batch insert size for epoch to block mapping
	defaultBatchSizeMappingInsert = 1000

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
	BnMax uint64 `gorm:"not null;index"`
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

// blockRange returns the spanning block range for the give epoch.
func (e2bms *epochBlockMapStore) BlockRange(epoch uint64) (citypes.RangeUint64, bool, error) {
	var e2bmap epochBlockMap
	var bnr citypes.RangeUint64

	existed, err := e2bms.exists(&e2bmap, "epoch = ?", epoch)
	if err != nil {
		return bnr, false, err
	}

	bnr.From, bnr.To = e2bmap.BnMin, e2bmap.BnMax
	return bnr, existed, nil
}

// ClosestEpochUpToBlock finds the nearest epoch with its ending block less than or equal to `blockNumber`.
func (e2bms *epochBlockMapStore) ClosestEpochUpToBlock(blockNumber uint64) (uint64, bool, error) {
	var result epochBlockMap
	err := e2bms.db.
		Select("epoch").
		Where("bn_max <= ?", blockNumber).
		Order("bn_max DESC").
		Take(&result).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}

	return result.Epoch, true, nil
}

// pivotHash returns the pivot hash of the given epoch.
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
		pivotBlock := data.GetPivotBlock()
		mappings = append(mappings, &epochBlockMap{
			Epoch:     data.Number,
			BnMin:     data.Blocks[0].BlockNumber.ToInt().Uint64(),
			BnMax:     pivotBlock.BlockNumber.ToInt().Uint64(),
			PivotHash: pivotBlock.Hash.String(),
		})
	}

	if len(mappings) == 0 {
		return nil
	}

	return dbTx.CreateInBatches(mappings, defaultBatchSizeMappingInsert).Error
}

// Remove remove epoch to block mappings of specific epoch range from db store.
func (e2bms *epochBlockMapStore) Remove(dbTx *gorm.DB, epochFrom, epochTo uint64) error {
	return dbTx.Where("epoch >= ? AND epoch <= ?", epochFrom, epochTo).Delete(&epochBlockMap{}).Error
}
