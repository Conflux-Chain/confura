package mysql

import (
	"fmt"

	"gorm.io/gorm"
)

type migrationStatus uint8

const (
	// Zero preserves old rows after an additive schema upgrade. Old writers
	// must be drained before new writers start recording persistent state.
	migrationLegacy migrationStatus = iota
	migrationShared
	migrationCompleted
)

// logMigrationCompleted deliberately bypasses dictionary caches. A completed
// migration remains completed even if rollback empties every dedicated table.
func logMigrationCompleted(db *gorm.DB, model interface{}, id uint64, entity string) (bool, error) {
	var row struct{ MigrationStatus migrationStatus }
	if err := db.Model(model).Select("migration_status").Where("id = ?", id).Take(&row).Error; err != nil {
		return false, err
	}
	switch row.MigrationStatus {
	case migrationShared:
		return false, nil
	case migrationCompleted:
		return true, nil
	case migrationLegacy:
		var partition bnPartition
		err := db.Where("entity = ?", entity).Order("pi ASC").Take(&partition).Error
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return !partition.IsInitial() || partition.Count > 0, nil
	default:
		return false, fmt.Errorf("invalid log migration status %d for %s", row.MigrationStatus, entity)
	}
}

func completeLogMigration(db *gorm.DB, model interface{}, id uint64) error {
	return db.Model(model).Where("id = ?", id).UpdateColumn("migration_status", migrationCompleted).Error
}

func validateLogMigrationSchema(db *gorm.DB) error {
	for _, model := range []interface{}{&Contract{}, &Topic{}} {
		if !db.Migrator().HasColumn(model, "migration_status") {
			return fmt.Errorf("log migration schema upgrade required: %T.migration_status", model)
		}
	}
	return nil
}
