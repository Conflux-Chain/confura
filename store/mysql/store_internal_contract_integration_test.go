//go:build integration

package mysql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestMySQLInternalReorgTransaction(t *testing.T) {
	db, _ := newIntegrationDB(t)
	s := NewInternalContractLogStore(db)
	require.NoError(t, db.Create(&InternalContractLog{Epoch: 7}).Error)
	require.NoError(t, db.Create(&CfxTraceSyncEpochBlockMap{Epoch: 7}).Error)
	reject := errors.New("reject rollback")
	rollback := func(tx *gorm.DB) error {
		if err := s.Pop(tx, 7); err != nil {
			return err
		}
		if err := tx.Where("epoch >= ?", 7).Delete(&CfxTraceSyncEpochBlockMap{}).Error; err != nil {
			return err
		}
		return s.IncrementReorgVersion(tx)
	}
	require.ErrorIs(t, db.Transaction(func(tx *gorm.DB) error {
		if err := rollback(tx); err != nil {
			return err
		}
		version, err := s.GetReorgVersion()
		require.NoError(t, err)
		require.Zero(t, version)
		require.EqualValues(t, 1, integrationCount(t, db, "internal_contract_logs"))
		return reject
	}), reject)
	version, err := s.GetReorgVersion()
	require.NoError(t, err)
	require.Zero(t, version)
	require.EqualValues(t, 1, integrationCount(t, db, "cfx_trace_sync_epoch_block_map"))
	require.NoError(t, db.Transaction(rollback))
	version, err = s.GetReorgVersion()
	require.NoError(t, err)
	require.Equal(t, 1, version)
	require.Zero(t, integrationCount(t, db, "internal_contract_logs"))
	require.Zero(t, integrationCount(t, db, "cfx_trace_sync_epoch_block_map"))

	// Separate transactions must not lose increments on the same version row.
	results := make(chan error, 8)
	for i := 0; i < cap(results); i++ {
		go func() { results <- db.Transaction(s.IncrementReorgVersion) }()
	}
	for i := 0; i < cap(results); i++ {
		require.NoError(t, <-results)
	}
	version, err = s.GetReorgVersion()
	require.NoError(t, err)
	require.Equal(t, 9, version)
}
