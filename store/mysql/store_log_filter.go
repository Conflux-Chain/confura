package mysql

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/conflux-chain/conflux-infura/store"
	"gorm.io/gorm"
)

type logColumnType int

const (
	logColumnTypeContract logColumnType = 0
	logColumnTypeTopic0   logColumnType = 1
	logColumnTypeTopic1   logColumnType = 2
	logColumnTypeTopic2   logColumnType = 3
	logColumnTypeTopic3   logColumnType = 4
)

var logWhereQueries = map[logColumnType]struct{ single, multiple string }{
	logColumnTypeContract: {"contract_address = ?", "contract_address IN (?)"},
	logColumnTypeTopic0:   {"topic0 = ?", "topic0 IN (?)"},
	logColumnTypeTopic1:   {"topic1 = ?", "topic1 IN (?)"},
	logColumnTypeTopic2:   {"topic2 = ?", "topic2 IN (?)"},
	logColumnTypeTopic3:   {"topic3 = ?", "topic3 IN (?)"},
}

func applyVariadicFilter(db *gorm.DB, column logColumnType, value store.VariadicValue) *gorm.DB {
	if single, ok := value.Single(); ok {
		return db.Where(logWhereQueries[column].single, single)
	}

	if multiple, ok := value.FlatMultiple(); ok {
		return db.Where(logWhereQueries[column].multiple, multiple)
	}

	return db
}

func applyTopicsFilter(db *gorm.DB, topics []store.VariadicValue) *gorm.DB {
	numTopics := len(topics)

	if numTopics > 0 {
		db = applyVariadicFilter(db, logColumnTypeTopic0, topics[0])
	}

	if numTopics > 1 {
		db = applyVariadicFilter(db, logColumnTypeTopic1, topics[1])
	}

	if numTopics > 2 {
		db = applyVariadicFilter(db, logColumnTypeTopic2, topics[2])
	}

	if numTopics > 3 {
		db = applyVariadicFilter(db, logColumnTypeTopic3, topics[3])
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
	db = db.Session(&gorm.Session{}).Offset(int(store.MaxLogLimit + filter.OffSet)).Limit(1)

	var hasMore int64
	if err := db.Select("1").Find(&hasMore).Error; err != nil {
		return err
	}

	if hasMore > 0 {
		return store.ErrGetLogsResultSetTooLarge
	}

	return nil
}
