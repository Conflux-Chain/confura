package mysql

import (
	"errors"
	"time"

	rate "github.com/Conflux-Chain/confura/util/rate/v2"
	"gorm.io/gorm"
)

var (
	errBreakFindInBatches = errors.New("break gorm DB `FindInBatches` loop")
)

// RateLimit rate limit keyset table
type RateLimit struct {
	ID        uint32
	SID       uint32 `gorm:"index"`                    // strategy ID
	LimitType int    `gorm:"default:0;not null"`       // limit type
	LimitKey  string `gorm:"unique;size:128;not null"` // limit key
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

func (rls *RateLimitStore) AddRateLimit(sid uint32, limitType rate.LimitType, limitKey string) error {
	ratelimit := &RateLimit{
		SID: sid, LimitType: int(limitType), LimitKey: limitKey,
	}

	return rls.db.Create(ratelimit).Error
}

func (rls *RateLimitStore) DeleteRateLimit(limitKey string) (bool, error) {
	res := rls.db.Delete(&RateLimit{}, "limit_key = ?", limitKey)
	return res.RowsAffected > 0, res.Error
}

func (rls *RateLimitStore) LoadRateLimitKeyset(filter *rate.KeysetFilter) (res []*rate.KeyInfo, err error) {
	db := rls.db

	if len(filter.KeySet) > 0 {
		db = db.Where("limit_key IN (?)", filter.KeySet)
	}

	if len(filter.SIDs) > 0 {
		db = db.Where("s_id IN (?)", filter.SIDs)
	}

	if db == rls.db && filter.Limit <= 0 {
		return nil, nil
	}

	var totals int
	var ratelimits []RateLimit

	rs := db.FindInBatches(&ratelimits, 200, func(tx *gorm.DB, batch int) error {
		for i := range ratelimits {
			res = append(res, &rate.KeyInfo{
				Type: rate.LimitType(ratelimits[i].LimitType),
				Key:  ratelimits[i].LimitKey,
				SID:  ratelimits[i].SID,
			})
		}

		totals += len(ratelimits)
		if filter.Limit > 0 && totals >= filter.Limit {
			// enough records, break the batch loop
			return errBreakFindInBatches
		}

		return nil
	})

	if rs.Error != errBreakFindInBatches {
		err = rs.Error
	}

	return
}
