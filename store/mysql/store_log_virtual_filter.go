package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util"
	w3types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

const (
	// Max number of archive log partitions of virtual filter to maintain. Once exceeded,
	// partitions will be dropped one by one from the oldest to keep the max archive limit.
	maxArchiveVirtualFilterLogPartitions = 1
)

// vflog virtual filter event logs
type vflog struct {
	ID              uint64
	BlockNumber     uint64 `gorm:"column:bn;not null;index:idx_bn"`
	BlockHash       string `gorm:"size:66;not null"`
	ContractAddress string `gorm:"size:66;not null"`
	Topic0          string `gorm:"size:66;not null"`
	Topic1          string `gorm:"size:66"`
	Topic2          string `gorm:"size:66"`
	Topic3          string `gorm:"size:66"`
	LogIndex        uint64 `gorm:"not null"`
	JsonRepr        []byte `gorm:"type:mediumText"` // marshalled json representation
	IsDel           bool   `gorm:"default:false"`   // soft delete flag

	fid string `gorm:"-"` // virtual filter ID
}

func newVflog(log *w3types.Log) (*vflog, error) {
	jdata, err := json.Marshal(log)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to json marshal")
	}

	return &vflog{
		BlockNumber:     log.BlockNumber,
		BlockHash:       log.BlockHash.String(),
		ContractAddress: log.Address.String(),
		Topic0:          log.Topics[0].String(),
		Topic1:          log.Topics[1].String(),
		Topic2:          log.Topics[2].String(),
		Topic3:          log.Topics[3].String(),
		LogIndex:        uint64(log.Index),
		JsonRepr:        jdata,
	}, nil

}

func (l vflog) TableName() string {
	if len(l.fid) == 0 { // in case filter id not provided
		return ""
	}

	return fmt.Sprintf("vflogs_%v", l.fid)
}

// vfLogFilter is used to query event logs for specified virtual filter through search criterions
// such as block number range, topics, contract address and block hashes.
type vfLogFilter struct {
	LogFilter

	BlockHashes []string
	Contracts   store.VariadicValue
}

func (filter *vfLogFilter) validateCount(db *gorm.DB) error {
	db = db.Where("is_del <> ", true)
	db = applyContractFilter(db, filter.Contracts)

	if len(filter.BlockHashes) > 0 {
		db = db.Where("block_hash IN (?)", filter.BlockHashes)
	}

	return filter.LogFilter.validateCount(db)
}

func (filter *vfLogFilter) Find(db *gorm.DB) ([]*vflog, error) {
	if err := filter.validateCount(db); err != nil {
		return nil, err
	}

	db = db.Table(filter.TableName).
		Where("is_del <> ?", true).
		Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo).
		Limit(int(store.MaxLogLimit) + 1)

	if len(filter.BlockHashes) > 0 {
		db = db.Where("block_hash IN (?)", filter.BlockHashes)
	}

	db = applyTopicsFilter(db, filter.Topics)
	db = applyContractFilter(db, filter.Contracts)

	var result []*vflog
	if err := db.Find(&result).Error; err != nil {
		return nil, err
	}

	if len(result) > int(store.MaxLogLimit) {
		return nil, store.ErrGetLogsResultSetTooLarge
	}

	return result, nil
}

// VirtualFilterLogStore partitioned store for virtual filter which polls filter changed data instantly.
type VirtualFilterLogStore struct {
	*bnPartitionedStore
}

func NewVirtualFilterLogStore(db *gorm.DB) *VirtualFilterLogStore {
	return &VirtualFilterLogStore{
		bnPartitionedStore: newBnPartitionedStore(db),
	}
}

// PreparePartitions createa new log partition for the virtual filter if necessary.
func (vfls *VirtualFilterLogStore) PreparePartition(fid string) (bnPartition, bool, error) {
	fentity, ftabler := vfls.filterEntity(fid), vfls.filterTabler(fid)
	partition, newCreated, err := vfls.autoPartition(fentity, ftabler, bnPartitionedLogVolumeSize)
	if err != nil {
		return partition, false, errors.WithMessage(err, "failed to auto partition")
	}

	return partition, newCreated, nil
}

// DeletePartitions deletes all table partitions for the virtual filter with specified id.
func (vfls *VirtualFilterLogStore) DeletePartitions(fid string) error {
	fentity, ftabler := vfls.filterEntity(fid), vfls.filterTabler(fid)
	_, err := vfls.deleteEntityPartitions(fentity, ftabler)

	return err
}

// Append appends incoming filter changed event logs, if some old ones of same block hashes already
// existed in the db, they will be soft deleted at first then the new ones will be inserted to
// the specified partition table.
func (vfls *VirtualFilterLogStore) Append(fid string, logs []w3types.Log, partition bnPartition) error {
	if len(logs) == 0 {
		return nil
	}

	vflogs := make([]*vflog, 0, len(logs))
	bnMin, bnMax := uint64(math.MaxUint64), uint64(0)

	var blockHashes []string
	bh2bnHashset := make(map[string]uint64)

	for i := range logs {
		bn := logs[i].BlockNumber
		bh := logs[i].BlockHash.String()

		if _, ok := bh2bnHashset[bh]; !ok {
			bh2bnHashset[bh] = bn
			blockHashes = append(blockHashes, bh)
		}

		vflog, err := newVflog(&logs[i])
		if err != nil {
			return err
		}

		vflogs = append(vflogs, vflog)
		bnMin, bnMax = util.MinUint64(bn, bnMin), util.MaxUint64(bn, bnMax)
	}

	fentity, ftabler := vfls.filterEntity(fid), vfls.filterTabler(fid)
	partitions, _, err := vfls.searchPartitions(fentity, types.RangeUint64{From: bnMin, To: bnMax})
	if err != nil {
		return errors.WithMessage(err, "failed to search partitions")
	}

	return vfls.db.Transaction(func(tx *gorm.DB) error {
		// soft delete event logs within db, whose block hashes coincides with the to be appended ones
		for i := len(partitions) - 1; i >= 0; i-- {
			tblName := vfls.getPartitionedTableName(ftabler, partitions[i].Index)

			dbtx := tx.Table(tblName).Where("bn BETWEEN ? AND ?", bnMin, bnMax).Where("bh IN (?)", blockHashes)
			if res := dbtx.Update("is_del", true); res.Error != nil {
				return errors.WithMessage(res.Error, "failed to soft delete deprecated logs")
			}
		}

		// batch insert new event logs
		tblName := vfls.getPartitionedTableName(ftabler, partition.Index)
		err = tx.Table(tblName).CreateInBatches(vflogs, defaultBatchSizeLogInsert).Error
		if err != nil {
			return err
		}

		// expand partition block number range
		err := vfls.expandPartitioBnRange(tx, fentity, partition.Index, bnMin, bnMax)
		if err != nil {
			return errors.WithMessage(err, "failed to expand partiton bn range")
		}

		// update partition data count
		err = vfls.deltaUpdateCount(tx, fentity, int(partition.Index), len(vflogs))
		if err != nil {
			return errors.WithMessage(err, "failed to delta update partition size")
		}

		return nil
	})
}

// GetLogs gets event logs for the specified virtual filter.
func (vfls *VirtualFilterLogStore) GetLogs(
	ctx context.Context, fid string, sfilter store.LogFilter, blockHashes ...string,
) ([]w3types.Log, error) {
	fentity, ftabler := vfls.filterEntity(fid), vfls.filterTabler(fid)

	srange := types.RangeUint64{From: sfilter.BlockFrom, To: sfilter.BlockTo}
	partitions, _, err := vfls.searchPartitions(fentity, srange)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to search partitions")
	}

	filter := &vfLogFilter{
		LogFilter: LogFilter{
			BlockFrom: sfilter.BlockFrom, BlockTo: sfilter.BlockTo, Topics: sfilter.Topics,
		},
		Contracts:   sfilter.Contracts,
		BlockHashes: blockHashes,
	}

	var result []w3types.Log
	for _, partition := range partitions {
		// check timeout before query
		select {
		case <-ctx.Done():
			return nil, store.ErrGetLogsTimeout
		default:
		}

		filter.TableName = vfls.getPartitionedTableName(ftabler, partition.Index)
		logs, err := filter.Find(vfls.db)
		if err != nil {
			return nil, err
		}

		// convert to web3go log type
		for _, v := range logs {
			var w3log w3types.Log
			if err := json.Unmarshal(v.JsonRepr, &w3log); err != nil {
				return nil, errors.WithMessage(err, "invalid event log json")
			}

			result = append(result, w3log)
		}

		// check log count
		if len(result) > int(store.MaxLogLimit) {
			return nil, store.ErrGetLogsResultSetTooLarge
		}
	}

	return result, nil
}

// GC garbage collects archive partitions exceeded the max archive partition limit
// starting from the oldest partiton.
func (vfls *VirtualFilterLogStore) GC(fid string) error {
	fentity, ftabler := vfls.filterEntity(fid), vfls.filterTabler(fid)
	_, err := vfls.pruneArchivePartitions(fentity, ftabler, maxArchiveVirtualFilterLogPartitions)

	return err
}

// expandPartitioBnRange expands block number range of the specified entity partition.
func (vfls *VirtualFilterLogStore) expandPartitioBnRange(dbTx *gorm.DB, entity string, partitionIndex uint32, from, to uint64) error {
	updates := map[string]interface{}{
		"bn_min": gorm.Expr("LEAST(IFNULL(bn_min, ?), ?)", math.MaxInt64, from),
		"bn_max": gorm.Expr("GREATEST(IFNULL(bn_max, ?), ?)", 0, to),
	}

	partition := bnPartition{Entity: entity, Index: partitionIndex}
	return dbTx.Where(&partition).Updates(updates).Error
}

// filterEntity gets partition entity of specified virtual proxy filter
func (vfls *VirtualFilterLogStore) filterEntity(fid string) string {
	return vflog{fid: fid}.TableName()
}

// filterTabler get partition tabler of specified virtual proxy filter
func (vfls *VirtualFilterLogStore) filterTabler(fid string) *vflog {
	return &vflog{fid: fid}
}
