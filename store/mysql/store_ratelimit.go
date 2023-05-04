package mysql

import (
	"time"

	"github.com/Conflux-Chain/confura/util/rate"
	"gorm.io/gorm"
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
) error {
	ratelimit := &RateLimit{
		SID:       sid,
		AclID:     aclId,
		LimitType: int(limitType),
		LimitKey:  limitKey,
		Memo:      memo,
	}

	return rls.db.Create(ratelimit).Error
}

func (rls *RateLimitStore) DeleteRateLimit(limitKey string) (bool, error) {
	res := rls.db.Delete(&RateLimit{}, "limit_key = ?", limitKey)
	return res.RowsAffected > 0, res.Error
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
