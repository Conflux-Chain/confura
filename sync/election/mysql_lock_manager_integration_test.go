//go:build integration

package election

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Conflux-Chain/go-conflux-util/dlock"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestMySQLLockManagerStrictRenewal(t *testing.T) {
	db, _ := newGuardTestDB(t)
	m := newMysqlLockManager(db)
	ctx := context.Background()
	intent := dlock.NewLockIntent("renew.test", "term-a", time.Minute)
	other := dlock.NewLockIntent(intent.Key, "term-b", intent.Lease)

	require.ErrorIs(t, m.Renew(ctx, intent), dlock.ErrLockNotHeld)
	require.NoError(t, m.Acquire(ctx, intent))
	require.ErrorIs(t, m.Acquire(ctx, other), dlock.ErrLockAcquisitionFailed)
	require.ErrorIs(t, m.Renew(ctx, other), dlock.ErrLockNotHeld)
	require.ErrorIs(t, m.Release(ctx, other), dlock.ErrLockNotHeld)
	require.NoError(t, m.Renew(ctx, intent))

	var before, after dlock.Dlock
	require.NoError(t, db.Where("`key` = ?", intent.Key).Take(&before).Error)
	failure := errors.New("abort authorized transaction")
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := m.RenewInTx(ctx, tx, intent); err != nil {
			return err
		}
		var renewed dlock.Dlock
		require.NoError(t, tx.Where("`key` = ?", intent.Key).Take(&renewed).Error)
		require.Equal(t, before.Version+1, renewed.Version)
		return failure
	})
	require.ErrorIs(t, err, failure)
	require.NoError(t, db.Where("`key` = ?", intent.Key).Take(&after).Error)
	require.Equal(t, before, after, "renewal must roll back with the caller's transaction")

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, m.Renew(canceled, intent), context.Canceled)
	require.ErrorIs(t, db.Transaction(func(tx *gorm.DB) error {
		return m.RenewInTx(canceled, tx, intent)
	}), context.Canceled)

	require.NoError(t, db.Model(&dlock.Dlock{}).Where("`key` = ?", intent.Key).
		Update("expired_at", gorm.Expr("UTC_TIMESTAMP(3) - INTERVAL 1 SECOND")).Error)
	require.ErrorIs(t, m.Renew(ctx, intent), dlock.ErrLockNotHeld)
	require.NoError(t, m.Acquire(ctx, other), "only acquisition may take over an expired lease")
	require.ErrorIs(t, m.Release(ctx, intent), dlock.ErrLockNotHeld)
	require.NoError(t, m.Renew(ctx, other))
	require.NoError(t, m.Release(ctx, other))
	require.ErrorIs(t, m.Renew(ctx, other), dlock.ErrLockNotHeld)
}

// Reproduce the old table on an isolated database, apply the documented offline
// migration, and exercise both the upstream acquirer and Confura's renewal path.
func TestMySQLLockMigrationAndUTCRenewal(t *testing.T) {
	db, _ := newGuardTestDB(t)
	require.NoError(t, db.Migrator().DropTable(&dlock.Dlock{}))
	require.NoError(t, db.Exec("CREATE TABLE dlocks (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, `key` VARCHAR(191) COLLATE utf8mb4_general_ci NOT NULL, nonce LONGTEXT NOT NULL, version BIGINT UNSIGNED NOT NULL DEFAULT 0, expired_at DATETIME(3) NOT NULL, created_at DATETIME(3), updated_at DATETIME(3), UNIQUE KEY uidx_key (`key`))").Error)
	require.NoError(t, db.Exec("INSERT INTO dlocks (`key`, nonce, expired_at) VALUES ('old-holder', 'old-nonce', NOW() + INTERVAL 1 HOUR)").Error)
	// Production requires stopping every old holder before this step.
	require.NoError(t, db.Exec("DELETE FROM dlocks").Error)
	require.NoError(t, db.AutoMigrate(&dlock.Dlock{}))
	var columns []struct {
		ColumnName string
		DataType   string
	}
	require.NoError(t, db.Raw("SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dlocks'").Scan(&columns).Error)
	types := make(map[string]string)
	for _, col := range columns {
		types[col.ColumnName] = col.DataType
	}
	require.Equal(t, "varbinary", types["key"])
	require.Equal(t, "longblob", types["nonce"])
	for _, zone := range []string{"+08:00", "-07:00"} {
		t.Run(zone, func(t *testing.T) {
			require.NoError(t, db.Connection(func(conn *gorm.DB) error {
				require.NoError(t, conn.Exec("SET time_zone = ?", zone).Error)
				defer conn.Exec("SET time_zone = '+00:00'")
				manager := newMysqlLockManager(conn)
				ctx := context.Background()
				intent := dlock.NewLockIntent("utc.test", "Owner", 30*time.Second+time.Microsecond)
				require.NoError(t, manager.Acquire(ctx, intent))
				require.NoError(t, manager.Renew(ctx, intent))
				var remaining int64
				require.NoError(t, conn.Raw("SELECT TIMESTAMPDIFF(MICROSECOND, UTC_TIMESTAMP(3), expired_at) FROM dlocks WHERE `key` = ?", intent.Key).Scan(&remaining).Error)
				require.Greater(t, remaining, int64(29*time.Second/time.Microsecond))
				require.LessOrEqual(t, remaining, int64(30*time.Second/time.Microsecond)+1000)
				wrongCase := dlock.NewLockIntent(intent.Key, "owner", intent.Lease)
				require.ErrorIs(t, manager.Renew(ctx, wrongCase), dlock.ErrLockNotHeld)
				require.ErrorIs(t, manager.Release(ctx, wrongCase), dlock.ErrLockNotHeld)
				require.NoError(t, conn.Model(&dlock.Dlock{}).Where("`key` = ?", intent.Key).Update("expired_at", gorm.Expr("UTC_TIMESTAMP(3)")).Error)
				require.ErrorIs(t, manager.Renew(ctx, intent), dlock.ErrLockNotHeld)
				require.NoError(t, manager.Release(ctx, intent))
				return nil
			}))
		})
	}
}
