//go:build integration

package mysql

import (
	"context"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestMySQLVirtualFilterReplayPartitions(t *testing.T) {
	db, _ := newIntegrationDB(t)
	s := NewVirtualFilterLogStore(db)
	const fid = "replay_test"
	first, _, err := s.PreparePartition(fid)
	require.NoError(t, err)
	original := []VirtualFilterLog{{BlockNumber: 10, BlockHash: "a", LogIndex: 0}, {BlockNumber: 20, BlockHash: "b", LogIndex: 0}}
	require.NoError(t, s.Append(fid, original, first))
	second, err := s.growPartition(s.filterEntity(fid), s.filterTabler(fid))
	require.NoError(t, err)
	replay := []VirtualFilterLog{{BlockNumber: 10, BlockHash: "a", LogIndex: 0, JsonRepr: []byte("replacement")}, {BlockNumber: 10, BlockHash: "c", LogIndex: 0}}
	require.NoError(t, s.Append(fid, replay, *second))
	filter := store.LogFilter{BlockFrom: 10, BlockTo: 20}
	logs, err := s.GetLogs(context.Background(), fid, filter)
	require.NoError(t, err)
	require.Len(t, logs, 3)
	require.Equal(t, "a", logs[0].BlockHash)
	require.Equal(t, []byte("replacement"), logs[0].JsonRepr)
	require.Equal(t, "c", logs[1].BlockHash)
	require.Equal(t, "b", logs[2].BlockHash)
	logs, err = s.GetLogs(context.Background(), fid, filter, "a")
	require.NoError(t, err)
	require.Len(t, logs, 1)

	// Commit a replay after the first table read. This query must retain its old
	// snapshot and return one b, rather than both old and replacement rows.
	replayed := false
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("replay-after-first-table", func(tx *gorm.DB) {
		if !replayed && tx.Statement.Table == "vflogs_replay_test_0" {
			if _, ok := tx.Statement.Dest.(*[]VirtualFilterLog); ok {
				replayed = true
				require.NoError(t, s.Append(fid, []VirtualFilterLog{{BlockNumber: 20, BlockHash: "b", JsonRepr: []byte("new-b")}}, *second))
			}
		}
	}))
	logs, err = s.GetLogs(context.Background(), fid, filter)
	require.NoError(t, db.Callback().Query().Remove("replay-after-first-table"))
	require.NoError(t, err)
	require.True(t, replayed)
	require.Len(t, logs, 3)
	require.Empty(t, logs[2].JsonRepr)
	logs, err = s.GetLogs(context.Background(), fid, filter, "b")
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, []byte("new-b"), logs[0].JsonRepr)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.GetLogs(ctx, fid, filter)
	require.Error(t, err)
	_, err = s.growPartition(s.filterEntity(fid), s.filterTabler(fid))
	require.NoError(t, err)
	require.NoError(t, s.GC(fid))
	require.False(t, db.Migrator().HasTable("vflogs_replay_test_0"))
	// The remaining replay partition still covers the lower replayed height.
	logs, err = s.GetLogs(context.Background(), fid, filter)
	require.NoError(t, err)
	require.Len(t, logs, 3)
	_, err = s.GetLogs(context.Background(), fid, store.LogFilter{BlockFrom: 9, BlockTo: 20})
	require.ErrorIs(t, err, store.ErrAlreadyPruned)
}

func TestMySQLVirtualFilterResultLimit(t *testing.T) {
	db, _ := newIntegrationDB(t)
	s := NewVirtualFilterLogStore(db)
	p, _, err := s.PreparePartition("limit_test")
	require.NoError(t, err)
	rows := make([]VirtualFilterLog, int(store.MaxLogLimit)+1)
	for i := range rows {
		rows[i] = VirtualFilterLog{BlockNumber: 10, BlockHash: "a", LogIndex: uint64(i)}
	}
	require.NoError(t, s.Append("limit_test", rows, p))
	logs, err := s.GetLogs(context.Background(), "limit_test", store.LogFilter{BlockFrom: 10, BlockTo: 10})
	require.ErrorIs(t, err, store.ErrFilterResultSetTooLarge)
	require.Empty(t, logs)
}
