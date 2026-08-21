package mysql

import (
	"context"

	"github.com/Conflux-Chain/confura/store"
	"gorm.io/gorm"
)

// scanLogFilter contains the predicates shared by all keyset log queries.
// Topic0ID is optional because only address-indexed and dedicated contract
// tables need to filter by topic ID; topic-only routing encodes the topic in
// either a required tid predicate or the dedicated table name.
type scanLogFilter struct {
	TableName string

	BlockFrom uint64
	BlockTo   uint64

	Topic0ID *uint64
}

func (filter *scanLogFilter) newQuery(ctx context.Context, db *gorm.DB) *gorm.DB {
	return db.WithContext(ctx).Table(filter.TableName)
}

func (filter *scanLogFilter) finishQuery(
	db *gorm.DB,
	cursor *store.ScanCursor,
	reverse bool,
	limit int,
	destSlicePtr any,
) error {
	if filter.Topic0ID != nil {
		db = db.Where("tid = ?", *filter.Topic0ID)
	}

	db = db.Where("bn BETWEEN ? AND ?", filter.BlockFrom, filter.BlockTo)

	if cursor != nil {
		if reverse {
			db = db.Where(
				"(bn < ? OR (bn = ? AND log_index < ?))",
				cursor.BlockNumber, cursor.BlockNumber, cursor.LogIndex,
			)
		} else {
			db = db.Where(
				"(bn > ? OR (bn = ? AND log_index > ?))",
				cursor.BlockNumber, cursor.BlockNumber, cursor.LogIndex,
			)
		}
	}

	if reverse {
		db = db.Order("bn DESC, log_index DESC")
	} else {
		db = db.Order("bn ASC, log_index ASC")
	}

	return db.Limit(limit).Find(destSlicePtr).Error
}

func (filter *scanLogFilter) find(
	ctx context.Context,
	db *gorm.DB,
	cursor *store.ScanCursor,
	reverse bool,
	limit int,
	destSlicePtr any,
) error {
	query := filter.newQuery(ctx, db)
	return filter.finishQuery(query, cursor, reverse, limit, destSlicePtr)
}

// AddressIndexedScanLogFilter scans one address hash partition. ContractID is
// still required because different addresses may share the same physical table.
type AddressIndexedScanLogFilter struct {
	scanLogFilter
	ContractID uint64
}

func (filter *AddressIndexedScanLogFilter) Find(
	ctx context.Context,
	db *gorm.DB,
	cursor *store.ScanCursor,
	reverse bool,
	limit int,
) ([]*AddressIndexedLog, error) {
	query := filter.newQuery(ctx, db).Where("cid = ?", filter.ContractID)

	var result []*AddressIndexedLog
	if err := filter.finishQuery(query, cursor, reverse, limit, &result); err != nil {
		return nil, err
	}

	return result, nil
}

// TopicIndexedScanLogFilter scans one topic hash partition. TopicID is still
// required because different topic hashes may share the same physical table.
type TopicIndexedScanLogFilter struct {
	scanLogFilter
	TopicID uint64
}

func (filter *TopicIndexedScanLogFilter) Find(
	ctx context.Context,
	db *gorm.DB,
	cursor *store.ScanCursor,
	reverse bool,
	limit int,
) ([]*TopicIndexedLog, error) {
	query := filter.newQuery(ctx, db).Where("tid = ?", filter.TopicID)

	var result []*TopicIndexedLog
	if err := filter.finishQuery(query, cursor, reverse, limit, &result); err != nil {
		return nil, err
	}

	return result, nil
}
