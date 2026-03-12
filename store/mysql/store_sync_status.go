package mysql

import (
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SyncStatus tracks the synchronization state as a singleton row.
type SyncStatus struct {
	ID           int       `gorm:"primaryKey;default:1;check:sync_status_singleton,id = 1"`
	ReorgVersion uint64    `gorm:"not null;default:0"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime"`
}

func (SyncStatus) TableName() string {
	return "sync_status"
}

type SyncStatusStore struct {
	*baseStore
}

func NewSyncStatusStore(db *gorm.DB) *SyncStatusStore {
	return &SyncStatusStore{baseStore: newBaseStore(db)}
}

func (s *SyncStatusStore) LoadReorgVersion(tx *gorm.DB) (uint64, error) {
	var status SyncStatus

	err := tx.First(&status, 1).Error
	if err != nil && !s.IsRecordNotFound(err) {
		return 0, err
	}

	return status.ReorgVersion, nil
}

func (s *SyncStatusStore) IncrementReorgVersion(tx *gorm.DB) error {
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"reorg_version": gorm.Expr("reorg_version + 1"),
		}),
	}).Create(&SyncStatus{
		ID:           1,
		ReorgVersion: 1,
	}).Error
}
