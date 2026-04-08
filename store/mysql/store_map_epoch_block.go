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
type epochBlockMapStore[T store.ChainData] struct {
	*baseStore

	// help partitioner
	partitioner *mysqlRangePartitioner
}

func newEpochBlockMapStore[T store.ChainData](db *gorm.DB, config *Config) *epochBlockMapStore[T] {
	return &epochBlockMapStore[T]{
		baseStore: newBaseStore(db),
		partitioner: newMysqlRangePartitioner(
			config.Database, epochBlockMap{}.TableName(), "epoch",
		),
	}
}

// preparePartition creates new table partition if necessary.
func (ebms *epochBlockMapStore[T]) preparePartition(dataSlice []T) error {
	var latestPartitionIndex int

	partition, err := ebms.partitioner.latestPartition(ebms.db)
	if err != nil {
		return errors.WithMessage(err, "failed to get latest partition")
	}

	if partition != nil {
		latestPartitionIndex = ebms.partitioner.indexOfPartition(partition)
	} else {
		// create initial partition
		initPartitionIndex := int(dataSlice[0].Number() / epochToBlockMappingPartitionSize)
		threshold := uint64(initPartitionIndex+1) * epochToBlockMappingPartitionSize

		err = ebms.partitioner.convert(ebms.db, initPartitionIndex, threshold)
		if err != nil {
			return errors.WithMessage(err, "failed to init range partitioned table")
		}

		latestPartitionIndex = int(initPartitionIndex)
	}

	for _, data := range dataSlice {
		if data.Number()%epochToBlockMappingPartitionSize != 0 {
			continue
		}

		// create new partition if necessary
		partitionIndex := int(data.Number() / epochToBlockMappingPartitionSize)
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
func (e2bms *epochBlockMapStore[T]) MaxEpoch() (uint64, bool, error) {
	var maxEpoch sql.NullInt64

	db := e2bms.db.Model(&epochBlockMap{}).Select("MAX(epoch)")
	if err := db.Scan(&maxEpoch).Error; err != nil {
		return 0, false, err
	}

	if !maxEpoch.Valid {
		return 0, false, nil
	}

	return uint64(maxEpoch.Int64), true, nil
}

// findOneBlockMapping retrieves a single `epochBlockMap` record based on a condition and optional ordering.
func (e2bms *epochBlockMapStore[T]) findOneBlockMapping(
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
func (e2bms *epochBlockMapStore[T]) FloorBlockMapping(epoch uint64) (epochBlockMap, bool, error) {
	return e2bms.findOneBlockMapping("epoch <= ?", "epoch DESC", epoch)
}

// CeilBlockMapping finds the `epochBlockMap` for the smallest epoch ≥ the given epoch.
func (e2bms *epochBlockMapStore[T]) CeilBlockMapping(epoch uint64) (epochBlockMap, bool, error) {
	return e2bms.findOneBlockMapping("epoch >= ?", "epoch ASC", epoch)
}

// BlockMapping retrieves the `epochBlockMap` for the exact given epoch.
func (e2bms *epochBlockMapStore[T]) BlockMapping(epoch uint64) (epochBlockMap, bool, error) {
	return e2bms.findOneBlockMapping("epoch = ?", "", epoch)
}

// LatestEpochBeforeBlock finds the latest epoch ≤ maxEpochNumber where BnMax ≤ blockNumber.
func (e2bms *epochBlockMapStore[T]) LatestEpochBeforeBlock(maxEpochNumber, blockNumber uint64) (uint64, bool, error) {
	res, existed, err := e2bms.findOneBlockMapping("epoch <= ? AND bn_max <= ?", "epoch DESC", maxEpochNumber, blockNumber)
	if err != nil || !existed {
		return 0, false, err
	}
	return res.Epoch, true, nil
}

// PivotHash returns the pivot hash of the given epoch.
func (e2bms *epochBlockMapStore[T]) PivotHash(epoch uint64) (string, bool, error) {
	var e2bmap epochBlockMap

	existed, err := e2bms.exists(&e2bmap, "epoch = ?", epoch)
	if err != nil {
		return "", false, err
	}

	return e2bmap.PivotHash, existed, nil
}

// Add batch saves epoch to block mapping data to db store.
func (e2bms *epochBlockMapStore[T]) Add(dbTx *gorm.DB, dataSlice []T) error {
	var mappings []*epochBlockMap

	for _, data := range dataSlice {
		blocks := data.ExtractBlocks()
		if len(blocks) == 0 {
			continue
		}

		pivotHash := data.Hash()

		firstBlock, endBlock := blocks[0], blocks[len(blocks)-1]
		mappings = append(mappings, &epochBlockMap{
			Epoch:     data.Number(),
			BnMin:     firstBlock.Number(),
			BnMax:     endBlock.Number(),
			PivotHash: pivotHash,
		})
	}

	if len(mappings) == 0 {
		return nil
	}

	return dbTx.Create(mappings).Error
}

// Remove remove epoch to block mappings of specific epoch range from db store.
func (e2bms *epochBlockMapStore[T]) Remove(dbTx *gorm.DB, epochFrom, epochTo uint64) error {
	return dbTx.Where("epoch >= ? AND epoch <= ?", epochFrom, epochTo).Delete(&epochBlockMap{}).Error
}

type CfxTraceSyncEpochBlockMap epochBlockMap

func (m CfxTraceSyncEpochBlockMap) TableName() string {
	return "cfx_trace_sync_epoch_block_map"
}

type CfxTraceSyncEpochBlockMapStore struct {
	*baseStore

	// help partitioner
	partitioner *mysqlRangePartitioner
}

func NewCfxTraceSyncEpochBlockMapStore(cs *CfxStore) *CfxTraceSyncEpochBlockMapStore {
	return &CfxTraceSyncEpochBlockMapStore{
		baseStore: newBaseStore(cs.DB()),
		partitioner: newMysqlRangePartitioner(
			cs.config.Database, CfxTraceSyncEpochBlockMap{}.TableName(), "epoch",
		),
	}
}

func (e2bms *CfxTraceSyncEpochBlockMapStore) MaxEpoch() (uint64, bool, error) {
	var maxEpoch sql.NullInt64

	db := e2bms.db.Model(&CfxTraceSyncEpochBlockMap{}).Select("MAX(epoch)")
	if err := db.Scan(&maxEpoch).Error; err != nil {
		return 0, false, err
	}

	if !maxEpoch.Valid {
		return 0, false, nil
	}

	return uint64(maxEpoch.Int64), true, nil
}

func (e2bms *CfxTraceSyncEpochBlockMapStore) Add(dbTx *gorm.DB, mappings []*CfxTraceSyncEpochBlockMap) error {
	if len(mappings) == 0 {
		return nil
	}

	minEpochNumber := mappings[0].Epoch
	maxEpochNumber := mappings[len(mappings)-1].Epoch

	if err := e2bms.preparePartition(minEpochNumber, maxEpochNumber); err != nil {
		return errors.WithMessage(err, "failed to prepare partitions")
	}

	return dbTx.Create(mappings).Error
}

func (e2bms *CfxTraceSyncEpochBlockMapStore) preparePartition(minEpochNumber, maxEpochNumber uint64) error {
	partition, err := e2bms.partitioner.latestPartition(e2bms.db)
	if err != nil {
		return errors.WithMessage(err, "failed to get latest partition")
	}

	var latestPartitionIndex int

	if partition != nil {
		latestPartitionIndex = e2bms.partitioner.indexOfPartition(partition)
	} else {
		// create initial partition
		initPartitionIndex := int(minEpochNumber / epochToBlockMappingPartitionSize)
		threshold := uint64(initPartitionIndex+1) * epochToBlockMappingPartitionSize

		err = e2bms.partitioner.convert(e2bms.db, initPartitionIndex, threshold)
		if err != nil {
			return errors.WithMessage(err, "failed to init range partitioned table")
		}

		latestPartitionIndex = initPartitionIndex
	}

	targetPartitionIndex := int(maxEpochNumber / epochToBlockMappingPartitionSize)

	for i := latestPartitionIndex + 1; i <= targetPartitionIndex; i++ {
		threshold := uint64(i+1) * epochToBlockMappingPartitionSize

		if err := e2bms.partitioner.addPartition(e2bms.db, i, threshold); err != nil {
			return errors.WithMessage(err, "failed to add partition")
		}
	}

	return nil
}

func (e2bms *CfxTraceSyncEpochBlockMapStore) LoadPivotHashes(fromEpoch, toEpoch uint64) (map[uint64]string, error) {
	if fromEpoch > toEpoch {
		return nil, errors.New("invalid epoch range")
	}

	if toEpoch-fromEpoch+1 > 100_000 {
		return nil, errors.New("epoch range too large")
	}

	var epochBlockMaps []CfxTraceSyncEpochBlockMap

	query := e2bms.db.Where("epoch BETWEEN ? AND ?", fromEpoch, toEpoch)
	if err := query.Find(&epochBlockMaps).Error; err != nil {
		return nil, err
	}

	pivotHashes := make(map[uint64]string, len(epochBlockMaps))
	for _, epochBlockMap := range epochBlockMaps {
		pivotHashes[epochBlockMap.Epoch] = epochBlockMap.PivotHash
	}
	return pivotHashes, nil
}

func (e2bms *CfxTraceSyncEpochBlockMapStore) Pop(dbTx *gorm.DB, epochUntil uint64) error {
	return dbTx.Where("epoch >= ?", epochUntil).Delete(&CfxTraceSyncEpochBlockMap{}).Error
}
