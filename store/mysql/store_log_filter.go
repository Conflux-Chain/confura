package mysql

import (
	"context"
	"fmt"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/pkg/errors"
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

func applyContractFilter(db *gorm.DB, contract store.VariadicValue) *gorm.DB {
	return applyVariadicFilter(db, logColumnTypeContract, contract)
}

func applyTopicsFilter(db *gorm.DB, topics []store.VariadicValue, skipTopics ...bool) *gorm.DB {
	numTopics := len(topics)
	skipTopic0 := len(skipTopics) > 0 && skipTopics[0]

	if numTopics > 0 && !skipTopic0 {
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

// LogFilter is used to query event logs with specified table, block number range and topics.
type LogFilter struct {
	TableName string

	// always indexed by block number
	BlockFrom uint64
	BlockTo   uint64

	// event hash and indexed data 1, 2, 3
	Topics []store.VariadicValue
}

// calculateQuerySetSize estimates the number of event logs matching the log filter, ignoring topics.
// Note: This is an approximation based on the auto-increment ID range. Due to possible chain reorgs,
// the estimate may be slightly inaccurate, but this is acceptable for our business use case.
func (filter *LogFilter) calculateQuerySetSize(db *gorm.DB) (types.RangeUint64, uint64, error) {
	// define subquery to retrieve the block range within the database based on filter criteria
	subQuery := db.
		Select("MIN(bn) AS minb, MAX(bn) AS maxb").
		Table(filter.TableName).
		Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo)

	// define main query to retrieve the auto-increment ID range within the filtered block range
	mainQuery := db.
		Select("MIN(t0.id) AS `from`, MAX(t0.id) AS `to`").
		Table(fmt.Sprintf("`%v` AS t0", filter.TableName)).
		Joins("INNER JOIN (?) AS t1 ON t0.bn IN (t1.minb, t1.maxb)", subQuery)

	// execute the main query to fetch the ID range
	var pidRange types.RangeUint64
	if err := mainQuery.Take(&pidRange).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return pidRange, 0, nil
		}
		return pidRange, 0, err
	}

	estimatedSize := pidRange.To - pidRange.From + 1
	return pidRange, estimatedSize, nil
}

// suggestBlockRange returns an adjusted block range that limits the query result size to be within `limitSize` records.
// If the original filter condition would produce more than `limitSize` records, this function identifies the first block
// within `queryRange` where the result size exceeds the limit. It then returns a range from `filter.BlockFrom` up to
// (but not including) that block. If no valid range can be suggested, it returns nil.
//
// It also includes the maximum possible epoch corresponding to the end of the suggested block range, which can be used by
// the caller for further inference, such as deriving a suggested epoch range if needed.
func (filter *LogFilter) suggestBlockRange(db *gorm.DB, queryRange types.RangeUint64, limitSize uint64) (*store.SuggestedBlockRange, error) {
	// no possible block range to suggest
	if filter.BlockFrom >= filter.BlockTo {
		return nil, nil
	}

	// fetch info on the first block exceeding `limitSize` within `queryRange`
	var exceedingBlock struct{ Bn, Epoch uint64 }
	err := db.Table(filter.TableName).
		Select("bn, epoch").
		Where("id >= ?", queryRange.From+limitSize).
		Order("id ASC").
		Take(&exceedingBlock).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	// suggest a narrower block range if possible
	if exceedingBlock.Bn > filter.BlockFrom {
		blockRange := store.NewSuggestedBlockRange(
			filter.BlockFrom, exceedingBlock.Bn-1, exceedingBlock.Epoch,
		)
		return &blockRange, nil
	}

	// no available block range to suggest
	return nil, nil
}

// validateQuerySetSize checks if the query set size exceeds limits, suggesting a narrower range if necessary.
func (filter *LogFilter) validateQuerySetSize(db *gorm.DB) error {
	// estimate the query range and log count in the dataset
	queryRange, numLogs, err := filter.calculateQuerySetSize(db)
	if err != nil {
		return err
	}

	// check if query size exceeds max allowed
	if numLogs > maxLogQuerySetSize {
		// suggest a narrower query range if topics filter is applied
		if filter.hasTopicsFilter() {
			suggestedRange, err := filter.suggestBlockRange(db, queryRange, maxLogQuerySetSize)
			if err != nil {
				return err
			}
			return store.NewSuggestedFilterQuerySetTooLargeError(suggestedRange)
		}
		// otherwise defer to result set size validation
	}

	// check if result set exceeds limit
	if numLogs > store.MaxLogLimit {
		// suggest a narrower range if no topics filter is applied
		if !filter.hasTopicsFilter() {
			suggestedRange, err := filter.suggestBlockRange(db, queryRange, store.MaxLogLimit)
			if err != nil {
				return err
			}
			return store.NewSuggestedFilterResultSetTooLargeError(suggestedRange)
		}

		// otherwise validate the count directly
		return filter.validateCount(db)
	}

	return nil
}

// validateCount validates the result set count against the configured max limit.
func (filter *LogFilter) validateCount(db *gorm.DB) error {
	db = db.Select("bn, epoch").
		Table(filter.TableName).
		Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo).
		Order("bn ASC").
		Offset(int(store.MaxLogLimit))
	db = applyTopicsFilter(db, filter.Topics)

	// fetch info on the first block exceeding `store.MaxLogLimit`
	var exceedingBlock struct{ Bn, Epoch uint64 }
	err := db.Take(&exceedingBlock).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}

	// suggest a narrower block range if possible
	if exceedingBlock.Bn > filter.BlockFrom {
		blockRange := store.NewSuggestedBlockRange(
			filter.BlockFrom, exceedingBlock.Bn-1, exceedingBlock.Epoch,
		)
		return store.NewSuggestedFilterResultSetTooLargeError(&blockRange)
	}

	return store.ErrFilterResultSetTooLarge
}

func (filter *LogFilter) hasTopicsFilter() bool {
	for _, v := range filter.Topics {
		if !v.IsNull() {
			return true
		}
	}

	return false
}

func (filter *LogFilter) find(ctx context.Context, db *gorm.DB, destSlicePtr interface{}) error {
	if store.IsBoundChecksEnabled(ctx) {
		if err := filter.validateQuerySetSize(db); err != nil {
			return err
		}
	}

	db = db.Table(filter.TableName)
	db = db.Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo)
	db = applyTopicsFilter(db, filter.Topics)
	db = db.Order("bn ASC")
	db = db.Limit(int(store.MaxLogLimit) + 1)

	return db.Find(destSlicePtr).Error
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

func (filter *AddressIndexedLogFilter) Find(ctx context.Context, db *gorm.DB) ([]*AddressIndexedLog, error) {
	if store.IsBoundChecksEnabled(ctx) {
		if err := filter.validateCount(db); err != nil {
			return nil, err
		}
	}

	db = db.Table(filter.TableName).
		Where("cid = ?", filter.ContractId).
		Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo).
		Order("bn ASC").
		Limit(int(store.MaxLogLimit) + 1)
	db = applyTopicsFilter(db, filter.Topics)

	var result []*AddressIndexedLog
	if err := db.Find(&result).Error; err != nil {
		return nil, err
	}

	return result, nil
}

// TopicIndexedLogFilter is used to query event logs indexed by topic id and block number.
type TopicIndexedLogFilter struct {
	LogFilter
	TopicId uint64
}

func (filter *TopicIndexedLogFilter) validateCount(db *gorm.DB) error {
	db = db.Where("tid = ?", filter.TopicId)
	return filter.LogFilter.validateCount(db)
}

func (filter *TopicIndexedLogFilter) Find(ctx context.Context, db *gorm.DB) ([]*TopicIndexedLog, error) {
	if store.IsBoundChecksEnabled(ctx) {
		if err := filter.validateCount(db); err != nil {
			return nil, err
		}
	}

	db = db.Table(filter.TableName).
		Where("tid = ?", filter.TopicId).
		Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo).
		Order("bn ASC").
		Limit(int(store.MaxLogLimit) + 1)
	db = applyTopicsFilter(db, filter.Topics, true)

	var result []*TopicIndexedLog
	if err := db.Find(&result).Error; err != nil {
		return nil, err
	}

	return result, nil
}
