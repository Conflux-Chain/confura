package store

import (
	"testing"

	citypes "github.com/Conflux-Chain/confura/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type mockChainData struct {
	number     uint64
	hash       string
	parentHash string
}

func (d mockChainData) Hash() string {
	return d.hash
}

func (d mockChainData) ParentHash() string {
	return d.parentHash
}

func (d mockChainData) Number() uint64 {
	return d.number
}

func (d mockChainData) ExtractBlocks() []BlockLike {
	return nil
}

func (d mockChainData) ExtractReceipts() map[string]ReceiptLike {
	return nil
}

func (d mockChainData) ExtractLogs() []LogLike {
	return nil
}

func TestRequireCanonicalContinuous(t *testing.T) {
	tests := []struct {
		name         string
		data         []mockChainData
		currentEpoch uint64
		currentHash  string
		wantErr      bool
	}{
		{
			name: "continuous from current canonical hash",
			data: []mockChainData{
				{number: 11, hash: "0x11", parentHash: "0x10"},
				{number: 12, hash: "0x12", parentHash: "0x11"},
			},
			currentEpoch: 10,
			currentHash:  "0x10",
		},
		{
			name: "continuous from empty store",
			data: []mockChainData{
				{number: 11, hash: "0x11", parentHash: "0x10"},
				{number: 12, hash: "0x12", parentHash: "0x11"},
			},
			currentEpoch: citypes.EpochNumberNil,
		},
		{
			name: "rejects number gap",
			data: []mockChainData{
				{number: 11, hash: "0x11", parentHash: "0x10"},
				{number: 13, hash: "0x13", parentHash: "0x11"},
			},
			currentEpoch: 10,
			currentHash:  "0x10",
			wantErr:      true,
		},
		{
			name: "rejects boundary parent mismatch",
			data: []mockChainData{
				{number: 11, hash: "0x11", parentHash: "0x09"},
			},
			currentEpoch: 10,
			currentHash:  "0x10",
			wantErr:      true,
		},
		{
			name: "rejects in batch parent mismatch",
			data: []mockChainData{
				{number: 11, hash: "0x11", parentHash: "0x10"},
				{number: 12, hash: "0x12", parentHash: "0x10"},
			},
			currentEpoch: 10,
			currentHash:  "0x10",
			wantErr:      true,
		},
		{
			name: "rejects missing canonical hash",
			data: []mockChainData{
				{number: 11, parentHash: "0x10"},
			},
			currentEpoch: 10,
			currentHash:  "0x10",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RequireCanonicalContinuous(tt.data, tt.currentEpoch, tt.currentHash)
			if tt.wantErr {
				require.True(t, errors.Is(err, ErrContinousEpochRequired))
				return
			}
			require.NoError(t, err)
		})
	}
}
