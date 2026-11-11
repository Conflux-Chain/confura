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
		Update("expired_at", gorm.Expr("NOW() - INTERVAL 1 SECOND")).Error)
	require.ErrorIs(t, m.Renew(ctx, intent), dlock.ErrLockNotHeld)
	require.NoError(t, m.Acquire(ctx, other), "only acquisition may take over an expired lease")
	require.ErrorIs(t, m.Release(ctx, intent), dlock.ErrLockNotHeld)
	require.NoError(t, m.Renew(ctx, other))
	require.NoError(t, m.Release(ctx, other))
	require.ErrorIs(t, m.Renew(ctx, other), dlock.ErrLockNotHeld)
}
