package mysql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Conflux-Chain/confura/util/rate"
	driver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// RateLimit rate limit keyset table
type RateLimit struct {
	ID        uint32
	SID       uint32 `gorm:"index"`                    // strategy ID
	AclID     uint32 `gorm:"index"`                    // allow list ID
	LimitType int    `gorm:"default:0;not null"`       // limit type
	LimitKey  string `gorm:"unique;size:128;not null"` // limit key
	Memo      string `gorm:"size:128"`                 // memo

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (RateLimit) TableName() string {
	return "ratelimits"
}

type RateLimitStore struct {
	*baseStore
}

func NewRateLimitStore(db *gorm.DB) *RateLimitStore {
	return &RateLimitStore{
		baseStore: newBaseStore(db),
	}
}

func (rls *RateLimitStore) AddRateLimit(
	sid uint32,
	aclId uint32,
	limitType rate.LimitType,
	limitKey string,
	memo string,
	expected ...*ConfigSnapshot,
) error {
	ratelimit := &RateLimit{
		SID:       sid,
		AclID:     aclId,
		LimitType: int(limitType),
		LimitKey:  limitKey,
		Memo:      memo,
	}

	// SQL errors and SQL trace output can include the full credential.
	quiet := NewRateLimitStore(rls.db.Session(&gorm.Session{Logger: rls.db.Logger.LogMode(logger.Silent)}))
	err := quiet.addRateLimitChecked(ratelimit, expected)
	var dbErr *driver.MySQLError
	if errors.As(err, &dbErr) {
		if dbErr.Number == 1062 {
			return errors.New("access key already exists")
		}
		return fmt.Errorf("write access key failed (MySQL error %d)", dbErr.Number)
	}
	return err
}

func (rls *RateLimitStore) DeleteRateLimit(limitKey string) (bool, error) {
	res := rls.db.Delete(&RateLimit{}, "limit_key = ?", limitKey)
	return res.RowsAffected > 0, res.Error
}

func (rls *RateLimitStore) DeleteRateLimitChecked(expected *RateLimit) (bool, error) {
	res := rls.db.Where("id = ? AND limit_key = ? AND s_id = ? AND acl_id = ? AND limit_type = ? AND memo = ? AND updated_at = ?",
		expected.ID, expected.LimitKey, expected.SID, expected.AclID, expected.LimitType, expected.Memo, expected.UpdatedAt).Delete(&RateLimit{})
	if res.Error != nil {
		return false, res.Error
	}
	if res.RowsAffected == 0 {
		return false, ErrConfigChanged
	}
	return true, nil
}

func (rls *RateLimitStore) LoadRateLimitKeyset(filter *rate.KeysetFilter) (res []*RateLimit, err error) {
	db := rls.db

	if len(filter.KeySet) > 0 {
		db = db.Where("limit_key IN (?)", filter.KeySet)
	}

	if len(filter.SIDs) > 0 {
		db = db.Where("s_id IN (?)", filter.SIDs)
	}

	if filter.Limit > 0 {
		db = db.Limit(filter.Limit)
	}

	var ratelimits []*RateLimit

	err = db.FindInBatches(&ratelimits, 200, func(tx *gorm.DB, batch int) error {
		res = append(res, ratelimits...)
		return nil
	}).Error

	return res, err
}

func (rls *RateLimitStore) LoadRateLimitKeyInfos(filter *rate.KeysetFilter) (res []*rate.KeyInfo, err error) {
	ratelimits, err := rls.LoadRateLimitKeyset(filter)
	if err != nil {
		return nil, err
	}

	for i := range ratelimits {
		res = append(res, &rate.KeyInfo{
			Type:  rate.LimitType(ratelimits[i].LimitType),
			Key:   ratelimits[i].LimitKey,
			SID:   ratelimits[i].SID,
			AclID: ratelimits[i].AclID,
		})
	}

	return res, nil
}

func (rls *RateLimitStore) LoadRateLimitKeyInfosContext(ctx context.Context, filter *rate.KeysetFilter) ([]*rate.KeyInfo, error) {
	return NewRateLimitStore(rls.db.WithContext(ctx)).LoadRateLimitKeyInfos(filter)
}
