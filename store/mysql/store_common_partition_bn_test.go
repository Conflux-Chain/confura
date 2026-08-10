package mysql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestSearchPartitionsUsesSingleMetadataQuery(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&bnPartition{}))
	require.NoError(t, db.Create([]bnPartition{
		{
			Entity: "logs", Index: 3,
			BnMin: sql.NullInt64{Int64: 100, Valid: true},
			BnMax: sql.NullInt64{Int64: 149, Valid: true},
		},
		{
			Entity: "logs", Index: 4,
			BnMin: sql.NullInt64{Int64: 150, Valid: true},
			BnMax: sql.NullInt64{Int64: 199, Valid: true},
		},
	}).Error)

	queryCount := 0
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register(
		"test:count-search-partition-queries", func(*gorm.DB) { queryCount++ },
	))

	partitionStore := newBnPartitionedStore(db)
	partitions, uncoverings, err := partitionStore.searchPartitions(
		context.Background(), "logs", types.RangeUint64{From: 140, To: 160},
	)
	require.NoError(t, err)
	assert.Equal(t, []uint32{3, 4}, partitionIndexes(partitions))
	assert.Nil(t, uncoverings)
	assert.Equal(t, 1, queryCount)
}

func TestSearchPartitionsFromMetadata(t *testing.T) {
	metadata := []*bnPartition{
		newTestBnPartition(3, 100, 149),
		newTestBnPartition(4, 150, 199),
		newTestBnPartition(5, 200, 249),
	}

	tests := []struct {
		name              string
		searchRange       types.RangeUint64
		wantIndexes       []uint32
		wantUncoverings   *types.RangeUint64
		wantAlreadyPruned bool
	}{
		{
			name:              "closed lower boundary is pruned",
			searchRange:       types.RangeUint64{From: 99, To: 100},
			wantAlreadyPruned: true,
		},
		{
			name:        "range within one partition",
			searchRange: types.RangeUint64{From: 110, To: 120},
			wantIndexes: []uint32{3},
		},
		{
			name:        "range spans adjacent partitions",
			searchRange: types.RangeUint64{From: 140, To: 210},
			wantIndexes: []uint32{3, 4, 5},
		},
		{
			name:            "range ends after metadata",
			searchRange:     types.RangeUint64{From: 240, To: 260},
			wantIndexes:     []uint32{5},
			wantUncoverings: &types.RangeUint64{From: 250, To: 260},
		},
		{
			name:            "range starts at metadata end",
			searchRange:     types.RangeUint64{From: 249, To: 260},
			wantIndexes:     []uint32{5},
			wantUncoverings: &types.RangeUint64{From: 250, To: 260},
		},
		{
			name:            "range starts after metadata",
			searchRange:     types.RangeUint64{From: 250, To: 260},
			wantUncoverings: &types.RangeUint64{From: 250, To: 260},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			partitions, uncoverings, err := searchPartitionsFromMetadata(
				"logs", test.searchRange, metadata,
			)
			if test.wantAlreadyPruned {
				require.Error(t, err)
				assert.ErrorIs(t, err, store.ErrAlreadyPruned)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, test.wantIndexes, partitionIndexes(partitions))
			assert.Equal(t, test.wantUncoverings, uncoverings)
		})
	}
}

func TestSearchPartitionsFromEmptyMetadata(t *testing.T) {
	searchRange := types.RangeUint64{From: 100, To: 200}
	partitions, uncoverings, err := searchPartitionsFromMetadata("logs", searchRange, nil)

	require.NoError(t, err)
	assert.Empty(t, partitions)
	assert.Equal(t, &searchRange, uncoverings)
}

func TestSearchPartitionsFromMetadataRejectsDiscontinuity(t *testing.T) {
	tests := []struct {
		name     string
		metadata []*bnPartition
	}{
		{
			name: "partition index gap",
			metadata: []*bnPartition{
				newTestBnPartition(3, 100, 149),
				newTestBnPartition(5, 150, 199),
			},
		},
		{
			name: "overlapping block ranges",
			metadata: []*bnPartition{
				newTestBnPartition(3, 100, 150),
				newTestBnPartition(4, 150, 199),
			},
		},
		{
			name: "inverted block range",
			metadata: []*bnPartition{
				newTestBnPartition(3, 150, 100),
			},
		},
		{
			name: "invalid range metadata",
			metadata: []*bnPartition{
				{Index: 3},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := searchPartitionsFromMetadata(
				"logs", types.RangeUint64{From: 100, To: 199}, test.metadata,
			)
			require.Error(t, err)
			assert.NotErrorIs(t, err, store.ErrAlreadyPruned)
		})
	}
}

func TestSearchPartitionsFromMetadataAllowsBlockGaps(t *testing.T) {
	metadata := []*bnPartition{
		newTestBnPartition(3, 100, 149),
		newTestBnPartition(4, 200, 249),
	}

	t.Run("range inside a no-data gap has no overlapping partition", func(t *testing.T) {
		partitions, uncoverings, err := searchPartitionsFromMetadata(
			"logs", types.RangeUint64{From: 160, To: 170}, metadata,
		)
		require.NoError(t, err)
		assert.Empty(t, partitions)
		assert.Nil(t, uncoverings)
	})

	t.Run("range spanning a no-data gap routes both partitions", func(t *testing.T) {
		partitions, uncoverings, err := searchPartitionsFromMetadata(
			"logs", types.RangeUint64{From: 140, To: 210}, metadata,
		)
		require.NoError(t, err)
		assert.Equal(t, []uint32{3, 4}, partitionIndexes(partitions))
		assert.Nil(t, uncoverings)
	})
}

func TestSearchPartitionsPruningSnapshotOutcomes(t *testing.T) {
	searchRange := types.RangeUint64{From: 50, To: 60}

	t.Run("snapshot before pruning routes the retained partition", func(t *testing.T) {
		metadata := []*bnPartition{
			newTestBnPartition(0, 0, 99),
			newTestBnPartition(1, 100, 199),
		}

		partitions, uncoverings, err := searchPartitionsFromMetadata("logs", searchRange, metadata)
		require.NoError(t, err)
		assert.Equal(t, []uint32{0}, partitionIndexes(partitions))
		assert.Nil(t, uncoverings)
	})

	t.Run("snapshot after pruning reports the range as pruned", func(t *testing.T) {
		metadata := []*bnPartition{
			newTestBnPartition(1, 100, 199),
		}

		_, _, err := searchPartitionsFromMetadata("logs", searchRange, metadata)
		require.Error(t, err)
		assert.ErrorIs(t, err, store.ErrAlreadyPruned)
	})
}

func newTestBnPartition(index uint32, from, to int64) *bnPartition {
	return &bnPartition{
		Index: index,
		BnMin: sql.NullInt64{Int64: from, Valid: true},
		BnMax: sql.NullInt64{Int64: to, Valid: true},
	}
}

func partitionIndexes(partitions []*bnPartition) []uint32 {
	if len(partitions) == 0 {
		return nil
	}

	indexes := make([]uint32, 0, len(partitions))
	for _, partition := range partitions {
		indexes = append(indexes, partition.Index)
	}
	return indexes
}
