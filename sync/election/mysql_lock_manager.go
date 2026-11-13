package election

import (
	"context"
	"errors"

	"github.com/Conflux-Chain/go-conflux-util/dlock"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// mysqlLockManager extends dlock.LockManager with strict, transactional renewal.
// Keep this extension independent of election state so it can move upstream.
type mysqlLockManager struct {
	*dlock.LockManager
	db *gorm.DB
}

func newMysqlLockManager(db *gorm.DB) *mysqlLockManager {
	return &mysqlLockManager{
		LockManager: dlock.NewLockManager(dlock.NewMySQLBackend(db)),
		db:          db,
	}
}

// Renew extends a live lease without reacquiring an expired or missing lock.
func (m *mysqlLockManager) Renew(ctx context.Context, intent *dlock.LockIntent) error {
	return m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return m.RenewInTx(ctx, tx, intent)
	})
}

// RenewInTx validates and extends a live lease, holding its row lock until the
// caller commits or rolls back. tx must be an active transaction on the same
// database as this manager. It must also contain the writes being authorized.
func (m *mysqlLockManager) RenewInTx(ctx context.Context, tx *gorm.DB, intent *dlock.LockIntent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx = tx.WithContext(ctx)
	// Lock first, then evaluate expiry in a new statement: NOW() in a statement
	// that waited for a row lock would otherwise refer to the start of that wait.
	var lease dlock.Dlock
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("`key` = ?", intent.Key).Take(&lease).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return dlock.ErrLockNotHeld
		}
		return err
	}
	result := tx.Model(&dlock.Dlock{}).
		Where("`key` = ? AND nonce = ? AND expired_at >= NOW()", intent.Key, intent.Nonce).
		Updates(map[string]interface{}{
			"expired_at": gorm.Expr("NOW() + INTERVAL ? SECOND", intent.Lease.Seconds()),
			"version":    gorm.Expr("version + 1"),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return dlock.ErrLockNotHeld
	}
	return nil
}
