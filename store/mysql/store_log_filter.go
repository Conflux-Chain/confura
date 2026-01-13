package mysql

import (
	"context"
	"fmt"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

const (
	maxLogQuerySetSize = 100_000
)

// TopicColumn defines the configuration of a single topic column
type TopicColumn struct {
	Name string // Column name, e.g. "topic0", "tid", this filter will be disabled if name is empty
}

// TopicSchema defines topic column mappings
type TopicSchema [4]TopicColumn

var (
	StandardTopicSchema      = TopicSchema{{"topic0"}, {"topic1"}, {"topic2"}, {"topic3"}}
	PrimaryIDTopicSchema     = TopicSchema{{"tid"}, {"topic1"}, {"topic2"}, {"topic3"}}
	SecondaryOnlyTopicSchema = TopicSchema{{}, {"topic1"}, {"topic2"}, {"topic3"}}
)

func applyVariadicFilter(db *gorm.DB, column string, value store.VariadicValuer) *gorm.DB {
	switch vals := value.Values(); len(vals) {
	case 0:
		return db
	case 1:
		return db.Where(fmt.Sprintf("%s = ?", column), vals[0])
	default:
		return db.Where(fmt.Sprintf("%s IN (?)", column), vals)
	}
}

func applyTopicsFilter(db *gorm.DB, topics []store.VariadicValuer, schemas ...*TopicSchema) *gorm.DB {
	schema := StandardTopicSchema
	if len(schemas) > 0 && schemas[0] != nil {
		schema = *schemas[0]
	}

	for i := 0; i < len(topics) && i < 4; i++ {
		if schema[i].Name != "" {
			db = applyVariadicFilter(db, schema[i].Name, topics[i])
		}
	}
	return db
}

// LogFilter is used to builds conditions to query event logs with specified table, block number range and topics.
type LogFilter struct {
	TableName string

	// always indexed by block number
	BlockFrom uint64
	BlockTo   uint64

	// event hash and indexed data 1, 2, 3
	Topics []store.VariadicValuer

	// topic columns schema
	Schema *TopicSchema
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
	db = applyTopicsFilter(db, filter.Topics, filter.Schema)

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
	schema := StandardTopicSchema
	if filter.Schema != nil {
		schema = *filter.Schema
	}

	for i, v := range filter.Topics {
		if !v.IsNull() && i < len(schema) && schema[i].Name != "" {
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
	db = applyTopicsFilter(db, filter.Topics, filter.Schema)
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
	db = applyTopicsFilter(db, filter.Topics, filter.Schema)

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
	db = applyTopicsFilter(db, filter.Topics, filter.Schema)

	var result []*TopicIndexedLog
	if err := db.Find(&result).Error; err != nil {
		return nil, err
	}

	return result, nil
}
