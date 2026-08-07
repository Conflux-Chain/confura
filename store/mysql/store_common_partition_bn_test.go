package mysql

import (
	"context"
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchPartitionsLowerBoundaryInclusive(t *testing.T) {
	readRange := scriptedBnRangeReader(
		bnPartitionRangeSnapshot{start: 100, end: 200, existed: true},
		bnPartitionRangeSnapshot{start: 100, end: 200, existed: true},
	)
	readOverlap := func(context.Context, string, types.RangeUint64) ([]*bnPartition, error) {
		t.Fatal("overlap query must not run for a pruned range")
		return nil, nil
	}

	_, _, err := searchPartitionsUntilStable(
		context.Background(), "logs", types.RangeUint64{From: 99, To: 100}, readRange, readOverlap,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, store.ErrAlreadyPruned)
}

func TestSearchPartitionsRetriesUntilMetadataStable(t *testing.T) {
	readRange := scriptedBnRangeReader(
		bnPartitionRangeSnapshot{start: 100, end: 200, existed: true},
		bnPartitionRangeSnapshot{start: 150, end: 200, existed: true},
		bnPartitionRangeSnapshot{start: 150, end: 200, existed: true},
		bnPartitionRangeSnapshot{start: 150, end: 200, existed: true},
	)

	overlapCalls := 0
	readOverlap := func(context.Context, string, types.RangeUint64) ([]*bnPartition, error) {
		overlapCalls++
		return []*bnPartition{{Index: uint32(overlapCalls)}}, nil
	}

	partitions, _, err := searchPartitionsUntilStable(
		context.Background(), "logs", types.RangeUint64{From: 150, To: 160}, readRange, readOverlap,
	)
	require.NoError(t, err)
	require.Len(t, partitions, 1)
	assert.Equal(t, uint32(2), partitions[0].Index)
	assert.Equal(t, 2, overlapCalls)
}

func TestSearchPartitionsRangeFingerprint(t *testing.T) {
	tests := []struct {
		name      string
		snapshots []bnPartitionRangeSnapshot
	}{
		{
			name: "end changed",
			snapshots: []bnPartitionRangeSnapshot{
				{start: 100, end: 200, existed: true},
				{start: 100, end: 201, existed: true},
				{start: 100, end: 201, existed: true},
				{start: 100, end: 201, existed: true},
			},
		},
		{
			name: "existence changed",
			snapshots: []bnPartitionRangeSnapshot{
				{existed: false},
				{start: 100, end: 200, existed: true},
				{start: 100, end: 200, existed: true},
				{start: 100, end: 200, existed: true},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			overlapCalls := 0
			readOverlap := func(context.Context, string, types.RangeUint64) ([]*bnPartition, error) {
				overlapCalls++
				return []*bnPartition{{Index: uint32(overlapCalls)}}, nil
			}

			partitions, _, err := searchPartitionsUntilStable(
				context.Background(), "logs", types.RangeUint64{From: 150, To: 160},
				scriptedBnRangeReader(test.snapshots...), readOverlap,
			)
			require.NoError(t, err)
			require.Len(t, partitions, 1)
			assert.Equal(t, uint32(2), partitions[0].Index)
			assert.Equal(t, 2, overlapCalls)
		})
	}
}

func TestSearchPartitionsRetriesWithoutFixedAttemptLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rangeReads := 0
	readRange := func(context.Context, string) (bnPartitionRangeSnapshot, error) {
		rangeReads++
		if rangeReads == 8 {
			cancel()
		}
		return bnPartitionRangeSnapshot{
			start:   uint64(rangeReads),
			end:     100,
			existed: true,
		}, nil
	}
	readOverlap := func(context.Context, string, types.RangeUint64) ([]*bnPartition, error) {
		return []*bnPartition{{Index: 1}}, nil
	}

	_, _, err := searchPartitionsUntilStable(
		ctx, "logs", types.RangeUint64{From: 50, To: 60}, readRange, readOverlap,
	)
	assert.ErrorIs(t, err, store.ErrGetLogsTimeout)
	assert.GreaterOrEqual(t, rangeReads, 8)
}

func TestSearchPartitionsDoesNotRetryOrdinaryErrors(t *testing.T) {
	expectedErr := errors.New("database unavailable")
	rangeCalls := 0
	readRange := func(context.Context, string) (bnPartitionRangeSnapshot, error) {
		rangeCalls++
		return bnPartitionRangeSnapshot{}, expectedErr
	}

	_, _, err := searchPartitionsUntilStable(
		context.Background(), "logs", types.RangeUint64{From: 1, To: 2}, readRange, nil,
	)
	assert.ErrorIs(t, err, expectedErr)
	assert.Equal(t, 1, rangeCalls)
}

func TestSearchPartitionsDoesNotRetryOverlapErrors(t *testing.T) {
	expectedErr := errors.New("database unavailable")
	rangeCalls := 0
	readRange := func(context.Context, string) (bnPartitionRangeSnapshot, error) {
		rangeCalls++
		return bnPartitionRangeSnapshot{start: 1, end: 100, existed: true}, nil
	}
	overlapCalls := 0
	readOverlap := func(context.Context, string, types.RangeUint64) ([]*bnPartition, error) {
		overlapCalls++
		return nil, expectedErr
	}

	_, _, err := searchPartitionsUntilStable(
		context.Background(), "logs", types.RangeUint64{From: 1, To: 2}, readRange, readOverlap,
	)
	assert.ErrorIs(t, err, expectedErr)
	assert.Equal(t, 1, rangeCalls)
	assert.Equal(t, 1, overlapCalls)
}

func scriptedBnRangeReader(snapshots ...bnPartitionRangeSnapshot) bnPartitionRangeReader {
	index := 0
	return func(context.Context, string) (bnPartitionRangeSnapshot, error) {
		if index >= len(snapshots) {
			return snapshots[len(snapshots)-1], nil
		}
		snapshot := snapshots[index]
		index++
		return snapshot, nil
	}
}
