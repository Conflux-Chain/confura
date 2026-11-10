//go:build integration

package mysql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
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

func TestMySQLInternalLogPages(t *testing.T) {
	db, _ := newIntegrationDB(t)
	s := NewInternalContractLogStore(db)
	var resultQueries int
	const queryCounter = "internal-log-result-queries"
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(queryCounter, func(tx *gorm.DB) {
		if _, ok := tx.Statement.Dest.(*[]InternalContractLog); ok {
			resultQueries++
			if store.IsBoundChecksEnabled(tx.Statement.Context) {
				require.NotContains(t, tx.Statement.SQL.String(), "ORDER BY")
			}
		}
	}))
	defer db.Callback().Query().Remove(queryCounter)
	require.NoError(t, db.Create(&CfxTraceSyncEpochBlockMap{Epoch: 7, BnMin: 10, BnMax: 11}).Error)
	filter := CfxInternalContractLogFilter{LogFilter: &types.LogFilter{
		FromEpoch: types.NewEpochNumberUint64(7), ToEpoch: types.NewEpochNumberUint64(7),
	}}
	for _, count := range []int{0, 1, 1000, 2000, 9999, 10000, 10001, 10002} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			require.NoError(t, db.Where("1 = 1").Delete(&InternalContractLog{}).Error)
			rows := make([]InternalContractLog, count)
			// Insertion order differs from execution order; each block spans two transactions.
			for i := range rows {
				rows[i] = InternalContractLog{Epoch: 7, BlockNumber: uint64(10 + i%2), LogIndex: i / 2, TxIndex: i % 2}
			}
			if count > 0 {
				require.NoError(t, db.Create(&rows).Error)
			}
			logs, err := s.GetLogs(store.NewContextWithBoundChecksDisabled(context.Background()), filter)
			require.NoError(t, err)
			require.Len(t, logs, count)
			ids := make(map[uint64]bool)
			for i, row := range logs {
				require.False(t, ids[row.ID])
				ids[row.ID] = true
				if i > 0 {
					prev := logs[i-1]
					require.True(t, prev.BlockNumber < row.BlockNumber || prev.BlockNumber == row.BlockNumber && prev.LogIndex < row.LogIndex)
				}
			}
			resultQueries = 0
			boundedLogs, err := s.GetLogs(context.Background(), filter)
			if count > int(store.MaxLogLimit) {
				require.ErrorIs(t, err, store.ErrFilterResultSetTooLarge)
				require.Empty(t, boundedLogs)
				require.Zero(t, resultQueries, "the bounds probe rejects oversized results")
			} else {
				require.NoError(t, err)
				require.Len(t, boundedLogs, count)
				if count > 0 {
					require.Equal(t, logs, boundedLogs)
				}
				require.Equal(t, 1, resultQueries, "bounded results use one query even at the limit")
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	logs, err := s.GetLogs(store.NewContextWithBoundChecksDisabled(canceled), filter)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, logs)

	// A failure on a later page must discard all previously read rows.
	failure := errors.New("second page failed")
	calls := 0
	const callback = "internal-log-page-failure"
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "internal_contract_logs" {
			calls++
			if calls == 2 {
				tx.AddError(failure)
			}
		}
	}))
	defer db.Callback().Query().Remove(callback)
	logs, err = s.GetLogs(store.NewContextWithBoundChecksDisabled(context.Background()), filter)
	require.ErrorIs(t, err, failure)
	require.Empty(t, logs)
}

func TestMySQLInternalLogPageTies(t *testing.T) {
	db, _ := newIntegrationDB(t)
	s := NewInternalContractLogStore(db)
	require.NoError(t, db.Create(&CfxTraceSyncEpochBlockMap{Epoch: 2, BnMin: 2, BnMax: 2}).Error)
	rows := make([]InternalContractLog, 4002)
	for i := range rows {
		// Matching logs share bn/li and have gaps between their IDs.
		rows[i] = InternalContractLog{Epoch: uint64(1 + i%2), BlockNumber: uint64(1 + i%2), LogIndex: 0}
	}
	require.NoError(t, db.Create(&rows).Error)
	var sizes []int64
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("page-query-size", func(tx *gorm.DB) {
		if tx.Statement.Table == "internal_contract_logs" {
			sql := tx.Statement.SQL.String()
			require.NotContains(t, sql, "bn >")
			require.NotContains(t, sql, "li >")
			sizes = append(sizes, tx.RowsAffected)
		}
	}))
	defer db.Callback().Query().Remove("page-query-size")
	logs, err := s.GetLogs(store.NewContextWithBoundChecksDisabled(context.Background()), CfxInternalContractLogFilter{LogFilter: &types.LogFilter{FromEpoch: types.NewEpochNumberUint64(1), ToEpoch: types.NewEpochNumberUint64(1)}})
	require.NoError(t, err)
	require.Len(t, logs, len(rows)/2)
	for i, row := range logs {
		require.Equal(t, rows[i*2].ID, row.ID)
	}
	require.Equal(t, []int64{1000, 1000, 1}, sizes)
}

func TestMySQLInternalLogsGrowAfterBoundsProbe(t *testing.T) {
	db, _ := newIntegrationDB(t)
	s := NewInternalContractLogStore(db)
	require.NoError(t, db.Create(&CfxTraceSyncEpochBlockMap{Epoch: 8, BnMin: 10, BnMax: 11}).Error)
	rows := make([]InternalContractLog, int(store.MaxLogLimit))
	for i := range rows {
		rows[i] = InternalContractLog{Epoch: 7, BlockNumber: 10, LogIndex: i}
	}
	require.NoError(t, db.Create(&rows).Error)

	appended := false
	var resultQueries int
	var resultRows int64
	const callback = "append-after-internal-log-bounds-probe"
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table != "internal_contract_logs" {
			return
		}
		if !appended && len(tx.Statement.Selects) > 0 {
			appended = true
			// The bounds probe saw exactly 10000 rows; two more arrive before
			// the result query. It must read only 10001 and return an error.
			require.NoError(t, db.Create(&[]InternalContractLog{
				{Epoch: 8, BlockNumber: 11, LogIndex: 0},
				{Epoch: 8, BlockNumber: 11, LogIndex: 1},
			}).Error)
		}
		if _, ok := tx.Statement.Dest.(*[]InternalContractLog); ok {
			resultQueries++
			resultRows += tx.RowsAffected
		}
	}))
	defer db.Callback().Query().Remove(callback)

	logs, err := s.GetLogs(context.Background(), CfxInternalContractLogFilter{LogFilter: &types.LogFilter{
		FromEpoch: types.NewEpochNumberUint64(7), ToEpoch: types.NewEpochNumberUint64(8),
	}})
	require.True(t, appended)
	require.ErrorIs(t, err, store.ErrFilterResultSetTooLarge)
	require.Empty(t, logs)
	require.Equal(t, 1, resultQueries)
	require.EqualValues(t, store.MaxLogLimit+1, resultRows)
}
