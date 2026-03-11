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

	tableName string
}

func (m epochBlockMap) TableName() string {
	if len(m.tableName) > 0 {
		return m.tableName
	}
	return "epoch_block_map"
}

var (
	cfxTraceSyncEpochBlockMapModel = epochBlockMap{
		tableName: "cfx_trace_sync_epoch_block_map",
	}
)

type CfxTraceSyncEpochBlockMapStore struct {
	*epochBlockMapStore[store.EpochData]
}

func NewCfxTraceSyncEpochBlockMapStore(cs *CfxStore) *CfxTraceSyncEpochBlockMapStore {
	return &CfxTraceSyncEpochBlockMapStore{
		epochBlockMapStore: newEpochBlockMapStore[store.EpochData](
			cs.DB(), cs.config, cfxTraceSyncEpochBlockMapModel,
		),
	}
}

func (e2bms *CfxTraceSyncEpochBlockMapStore) LoadPivotHashes(fromEpoch, toEpoch uint64) (map[uint64]string, error) {
	var epochBlockMaps []epochBlockMap

	query := e2bms.db.Where("epoch BETWEEN ? AND ?", fromEpoch, toEpoch)
	if err := query.Table(e2bms.model.TableName()).Find(&epochBlockMaps).Error; err != nil {
		return nil, err
	}

	pivotHashes := make(map[uint64]string, len(epochBlockMaps))
	for _, epochBlockMap := range epochBlockMaps {
		pivotHashes[epochBlockMap.Epoch] = epochBlockMap.PivotHash
	}
	return pivotHashes, nil
}

func (e2bms *CfxTraceSyncEpochBlockMapStore) Add(dbTx *gorm.DB, dataSlice []store.EpochData) error {
	if err := e2bms.preparePartition(dataSlice); err != nil {
		return errors.WithMessage(err, "failed to prepare partitions")
	}
	return e2bms.epochBlockMapStore.Add(dbTx, dataSlice)
}

func (e2bms *CfxTraceSyncEpochBlockMapStore) Pop(dbTx *gorm.DB, epochUntil uint64) error {
	return dbTx.Where("epoch >= ?", epochUntil).Delete(&e2bms.model).Error
}

// epochBlockMapStore used to get epoch to block map data.
type epochBlockMapStore[T store.ChainData] struct {
	*baseStore

	// help partitioner
	partitioner *mysqlRangePartitioner
	// model name
	model epochBlockMap
}

func newEpochBlockMapStore[T store.ChainData](db *gorm.DB, config *Config, model epochBlockMap) *epochBlockMapStore[T] {
	return &epochBlockMapStore[T]{
		model:     model,
		baseStore: newBaseStore(db),
		partitioner: newMysqlRangePartitioner(
			config.Database, model.TableName(), "epoch",
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

	db := e2bms.db.Table(e2bms.model.TableName()).Select("MAX(epoch)")
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

	err = query.Table(e2bms.model.TableName()).Take(&res).Error
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
	e2bmap := e2bms.model

	existed, err := e2bms.exists(&e2bmap, "epoch = ?", epoch)
	if err != nil {
		return "", false, err
	}

	return e2bmap.PivotHash, existed, nil
}

// Add batch saves epoch to block mapping data to db store.
func (e2bms *epochBlockMapStore[T]) Add(dbTx *gorm.DB, dataSlice []T) error {
	mappings := make([]*epochBlockMap, 0, len(dataSlice))

	for _, data := range dataSlice {
		blocks := data.ExtractBlocks()
		if len(blocks) == 0 {
			continue
		}

		firstBlock, endBlock := blocks[0], blocks[len(blocks)-1]
		mappings = append(mappings, &epochBlockMap{
			Epoch:     data.Number(),
			BnMin:     firstBlock.Number(),
			BnMax:     endBlock.Number(),
			PivotHash: data.Hash(),
		})
	}

	if len(mappings) == 0 {
		return nil
	}

	return dbTx.Table(e2bms.model.TableName()).Create(mappings).Error
}

// Remove remove epoch to block mappings of specific epoch range from db store.
func (e2bms *epochBlockMapStore[T]) Remove(dbTx *gorm.DB, epochFrom, epochTo uint64) error {
	return dbTx.Where("epoch >= ? AND epoch <= ?", epochFrom, epochTo).Delete(&e2bms.model).Error
}
