package sync

import (
	"math/big"
	"testing"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/assert"
)

func TestNewEpochPivotWindow(t *testing.T) {
	// create an uninitialized epoch pivot window
	w := newEpochPivotWindow(1000)
	assert.Equal(t, uint32(0), w.size())
	assert.False(t, w.isSet())

	// test expandTo
	w.expandTo(1000)
	assert.Equal(t, uint32(1), w.size())
	assert.True(t, w.isSet())

	w.expandTo(1001)
	assert.Equal(t, uint32(2), w.size())

	// test reset
	w.reset()
	assert.Equal(t, uint32(0), w.size())
	assert.False(t, w.isSet())
}

func TestEpochPivotWindowPushPop(t *testing.T) {
	w := newEpochPivotWindow(2)
	assert.False(t, w.isSet())

	// test push
	err := w.push(&types.Block{
		BlockHeader: types.BlockHeader{
			EpochNumber: (*hexutil.Big)(big.NewInt(0)),
			ParentHash:  "",
			Hash:        "epoch0",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), w.size())
	assert.Equal(t, uint64(0), w.epochFrom)
	assert.Equal(t, uint64(0), w.epochTo)
	assert.True(t, w.isSet())

	err = w.push(&types.Block{
		BlockHeader: types.BlockHeader{
			EpochNumber: (*hexutil.Big)(big.NewInt(1)),
			ParentHash:  "epoch0",
			Hash:        "epoch1",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), w.size())
	assert.Equal(t, uint64(0), w.epochFrom)
	assert.Equal(t, uint64(1), w.epochTo)

	// test push error - mismatched parent hash
	err = w.push(&types.Block{
		BlockHeader: types.BlockHeader{
			EpochNumber: (*hexutil.Big)(big.NewInt(2)),
			ParentHash:  "epoch1`",
			Hash:        "epoch2`",
		},
	})
	assert.Error(t, err)
	t.Logf("push error: %v", err)

	// test push error - incontinuous epoch
	err = w.push(&types.Block{
		BlockHeader: types.BlockHeader{
			EpochNumber: (*hexutil.Big)(big.NewInt(20)),
			ParentHash:  "epoch1",
			Hash:        "epoch2",
		},
	})
	assert.Error(t, err)
	t.Logf("push error: %v", err)

	// test auto reclaim over capacity on push
	err = w.push(&types.Block{
		BlockHeader: types.BlockHeader{
			EpochNumber: (*hexutil.Big)(big.NewInt(2)),
			ParentHash:  "epoch1",
			Hash:        "epoch2",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), w.size())
	assert.Equal(t, uint64(1), w.epochFrom)
	assert.Equal(t, uint64(2), w.epochTo)

	// test getPivotHash
	ph, ok := w.getPivotHash(0)
	assert.False(t, ok)
	assert.NotEqual(t, "epoch0", string(ph))

	ph, _ = w.getPivotHash(1)
	assert.Equal(t, "epoch1", string(ph))

	ph, _ = w.getPivotHash(2)
	assert.Equal(t, "epoch2", string(ph))

	// test pop empty
	w.popn(3)
	assert.Equal(t, uint32(2), w.size())

	// test pop underflow
	w.popn(0)
	assert.Equal(t, uint32(0), w.size())
	assert.False(t, w.isSet())
}
