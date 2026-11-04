package sync

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindCommonAncestor(t *testing.T) {
	hashes := map[uint64]string{
		7: "0x07",
		8: "0x08",
		9: "0x09",
	}

	ancestor, found, err := findCommonAncestor(9, "0x08", func(number uint64) (string, bool, error) {
		hash, ok := hashes[number]
		return hash, ok, nil
	})

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(8), ancestor)
}

func TestFindCommonAncestorSkipsMissingHashes(t *testing.T) {
	hashes := map[uint64]string{
		7: "0x07",
		9: "0x09",
	}

	ancestor, found, err := findCommonAncestor(9, "0x07", func(number uint64) (string, bool, error) {
		hash, ok := hashes[number]
		return hash, ok, nil
	})

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(7), ancestor)
}

func TestFindCommonAncestorNotFound(t *testing.T) {
	ancestor, found, err := findCommonAncestor(2, "0x09", func(number uint64) (string, bool, error) {
		return "", false, nil
	})

	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, ancestor)
}

func TestFindCommonAncestorPropagatesError(t *testing.T) {
	wantErr := errors.New("read failed")

	_, found, err := findCommonAncestor(2, "0x09", func(number uint64) (string, bool, error) {
		return "", false, wantErr
	})

	require.ErrorIs(t, err, wantErr)
	require.False(t, found)
}
