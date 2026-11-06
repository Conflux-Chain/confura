package mysql

import (
	"fmt"

	"github.com/Conflux-Chain/confura/store"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
	return logMigrationStateCompleted(db, row.MigrationStatus, entity)
}

func logMigrationStateCompleted(db *gorm.DB, status migrationStatus, entity string) (bool, error) {
	switch status {
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
		return false, fmt.Errorf("invalid log migration status %d for %s", status, entity)
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

// The row lock is the in-progress marker. It must be acquired before any
// snapshot read or log mutation; a crash releases it and rolls back the copy.
func lockLogMigration(db *gorm.DB, model interface{}, id uint64) error {
	var row struct{ ID uint64 }
	return db.Model(model).Select("id").Where("id = ?", id).
		Clauses(clause.Locking{Strength: "UPDATE"}).Take(&row).Error
}

// Prepared routes can become stale while another instance migrates. Lock all
// affected identities before writing, and reject a stale route for retry.
func (ms *MysqlStore[T]) lockLogWriteRoutes(db *gorm.DB, data []T, contracts, topics map[uint64]bnPartition) error {
	addresses := extractUniqueContractAddresses(data...)
	hashes := extractUniqueTopic0Signatures(data...)
	var addressValues, hashValues []string
	for address := range addresses {
		addressValues = append(addressValues, address)
	}
	for hash := range hashes {
		hashValues = append(hashValues, hash)
	}
	var contractRows []Contract
	if len(addressValues) > 0 {
		if err := db.Clauses(clause.Locking{Strength: "UPDATE"}).Where("address IN ?", addressValues).Order("id").Find(&contractRows).Error; err != nil {
			return err
		}
		if len(contractRows) != len(addressValues) {
			return store.ErrNotFound
		}
	}
	var topicRows []Topic
	if len(hashValues) > 0 {
		if err := db.Clauses(clause.Locking{Strength: "UPDATE"}).Where("hash IN ?", hashValues).Order("id").Find(&topicRows).Error; err != nil {
			return err
		}
		if len(topicRows) != len(hashValues) {
			return store.ErrNotFound
		}
	}
	for _, row := range contractRows {
		done, err := logMigrationStateCompleted(db, row.MigrationStatus, ms.bcls.contractEntity(row.ID))
		if err != nil {
			return err
		}
		_, prepared := contracts[row.ID]
		if done != prepared {
			return fmt.Errorf("contract %d log route changed; retry batch", row.ID)
		}
	}
	for _, row := range topicRows {
		done, err := logMigrationStateCompleted(db, row.MigrationStatus, ms.btls.topicEntity(row.ID))
		if err != nil {
			return err
		}
		_, prepared := topics[row.ID]
		if done != prepared {
			return fmt.Errorf("topic %d log route changed; retry batch", row.ID)
		}
	}
	return nil
}

func lockLogRollback(db *gorm.DB, epoch uint64) error {
	var contracts []Contract
	if err := db.Clauses(clause.Locking{Strength: "UPDATE"}).Where("latest_updated_epoch >= ?", epoch).Order("id").Find(&contracts).Error; err != nil {
		return err
	}
	var topics []Topic
	return db.Clauses(clause.Locking{Strength: "UPDATE"}).Where("latest_updated_epoch >= ?", epoch).Order("id").Find(&topics).Error
}
