package mysql

import (
	"database/sql"

	"github.com/Conflux-Chain/confura/store"
	"gorm.io/gorm"
)

type logColumnType int

const (
	logColumnTypeContract logColumnType = 0
	logColumnTypeTopic0   logColumnType = 1
	logColumnTypeTopic1   logColumnType = 2
	logColumnTypeTopic2   logColumnType = 3
	logColumnTypeTopic3   logColumnType = 4

	maxLogQuerySetSize = 100_000
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

// logFilter is used to query event logs with specified table, block number range and topics.
type LogFilter struct {
	TableName string

	// always indexed by block number
	BlockFrom uint64
	BlockTo   uint64

	// event hash and indexed data 1, 2, 3
	Topics []store.VariadicValue
}

// calculateQuerySetSize returns the number of event logs of specified block number range
// (without topics filter).
//
// Note, the result is not accurate, because some records are popped during chain reorg.
// However, it is tolerable for the business.
func (filter *LogFilter) calculateQuerySetSize(db *gorm.DB) (uint64, error) {
	db = db.Select("MIN(id) AS min, MAX(id) AS max").
		Table(filter.TableName).
		Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo)

	var result struct {
		Min sql.NullInt64
		Max sql.NullInt64
	}

	if err := db.Find(&result).Error; err != nil {
		return 0, err
	}

	if result.Min.Valid && result.Max.Valid {
		return uint64(result.Max.Int64 - result.Min.Int64 + 1), nil
	}

	return 0, nil
}

// validateCount validates the result set count against the configured max limit.
func (filter *LogFilter) validateCount(db *gorm.DB) error {
	db = db.Select("id").
		Table(filter.TableName).
		Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo).
		Offset(int(store.MaxLogLimit)).
		Limit(1)

	db = applyTopicsFilter(db, filter.Topics)

	var ids []uint64
	if err := db.Find(&ids).Error; err != nil {
		return err
	}

	if len(ids) > 0 {
		return store.ErrGetLogsResultSetTooLarge
	}

	return nil
}

func (filter *LogFilter) hasTopicsFilter() bool {
	for _, v := range filter.Topics {
		if !v.IsNull() {
			return true
		}
	}

	return false
}

func (filter *LogFilter) find(db *gorm.DB, destSlicePtr interface{}) error {
	numLogs, err := filter.calculateQuerySetSize(db)
	if err != nil {
		return err
	}

	// limit the query set size
	if numLogs > maxLogQuerySetSize {
		return store.ErrGetLogsQuerySetTooLarge
	}

	// validate the number of event logs if query set size exceeds the max limit
	if numLogs > store.MaxLogLimit {
		if !filter.hasTopicsFilter() {
			return store.ErrGetLogsResultSetTooLarge
		}

		// validate count if topics filter specified
		if err = filter.validateCount(db); err != nil {
			return err
		}
	}

	db = db.Table(filter.TableName)
	db = db.Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo)
	db = applyTopicsFilter(db, filter.Topics)
	db = db.Limit(int(store.MaxLogLimit) + 1)

	return db.Find(destSlicePtr).Error
}

// TODO add method FindXxx for type safety and double check the result set size <= max_limit.
func (filter *LogFilter) Find(db *gorm.DB) ([]int, error) {
	var result []int
	if err := filter.find(db, &result); err != nil {
		return nil, err
	}

	if len(result) > int(store.MaxLogLimit) {
		return nil, store.ErrGetLogsResultSetTooLarge
	}

	return result, nil
}

// AddressIndexedLogFilter is used to query event logs that indexed by contract id and block number.
type AddressIndexedLogFilter struct {
	LogFilter

	ContractId uint64
}

func (filter *AddressIndexedLogFilter) validateCount(db *gorm.DB) error {
	db = db.Where("cid = ?", filter.ContractId)
	return filter.LogFilter.validateCount(db)
}

func (filter *AddressIndexedLogFilter) Find(db *gorm.DB) ([]*AddressIndexedLog, error) {
	if err := filter.validateCount(db); err != nil {
		return nil, err
	}

	db = db.Table(filter.TableName).
		Where("cid = ?", filter.ContractId).
		Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo).
		Limit(int(store.MaxLogLimit) + 1)
	db = applyTopicsFilter(db, filter.Topics)

	var result []*AddressIndexedLog
	if err := db.Find(&result).Error; err != nil {
		return nil, err
	}

	if len(result) > int(store.MaxLogLimit) {
		return nil, store.ErrGetLogsResultSetTooLarge
	}

	return result, nil
}
