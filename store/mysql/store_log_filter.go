package mysql

import (
	"fmt"

	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/conflux-chain/conflux-infura/store"
	"gorm.io/gorm"
)

func applyVariadicFilter(db *gorm.DB, column string, value store.VariadicValue) *gorm.DB {
	if single, ok := value.Single(); ok {
		return db.Where(fmt.Sprintf("%v = ?", column), single)
	}

	if multiple, ok := value.FlatMultiple(); ok {
		return db.Where(fmt.Sprintf("%v IN (?)", column), multiple)
	}

	return db
}

func applyTopicsFilter(db *gorm.DB, topics []store.VariadicValue) *gorm.DB {
	numTopics := len(topics)

	if numTopics > 0 {
		db = applyVariadicFilter(db, "topic0", topics[0])
	}

	if numTopics > 1 {
		db = applyVariadicFilter(db, "topic1", topics[1])
	}

	if numTopics > 2 {
		db = applyVariadicFilter(db, "topic2", topics[2])
	}

	if numTopics > 3 {
		db = applyVariadicFilter(db, "topic3", topics[3])
	}

	return db
}

type BaseLogFilter struct {
	Topics []store.VariadicValue // event hash and indexed data 1, 2, 3
	OffSet uint64
	Limit  uint64
}

func (filter *BaseLogFilter) apply(db *gorm.DB) *gorm.DB {
	db = applyTopicsFilter(db, filter.Topics)

	if filter.OffSet > 0 {
		db = db.Offset(int(filter.OffSet))
	}

	if filter.Limit > 0 {
		db = db.Limit(int(filter.Limit))
	}

	// IMPORTANT: full node returns the last N logs.
	// To limit the number of records fetched for better performance,  we'd better retrieve
	// the logs in reverse order first, and then reverse them for the final order.
	return db.Order("id DESC")
}

type AddressIndexedLogFilter struct {
	BaseLogFilter
	Contract  cfxaddress.Address
	BlockFrom uint64
	BlockTo   uint64

	contractId uint64
	tableName  string
}

func (filter *AddressIndexedLogFilter) normalize(cs *ContractStore, partitionedTableName string) (bool, error) {
	contract, ok, err := cs.GetContractByAddress(filter.Contract.MustGetBase32Address())
	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	filter.contractId = contract.ID
	filter.tableName = partitionedTableName

	return true, nil
}

func (filter *AddressIndexedLogFilter) apply(db *gorm.DB) *gorm.DB {
	return db.Table(filter.tableName).
		Where("cid = ?", filter.contractId).
		Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo)
}

func (filter *AddressIndexedLogFilter) Apply(db *gorm.DB) *gorm.DB {
	db = filter.apply(db)
	return filter.BaseLogFilter.apply(db)
}

func (filter *AddressIndexedLogFilter) ValidateCount(db *gorm.DB) error {
	db = filter.apply(db)
	db = applyTopicsFilter(db, filter.Topics)
	db = db.Offset(int(store.MaxLogLimit + filter.OffSet)).Limit(1)

	var total int64
	if err := db.Count(&total).Error; err != nil {
		return err
	}

	if total > 0 {
		return errTooManyLogs
	}

	return nil
}
