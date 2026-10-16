package mysql

import (
	"testing"

	"github.com/Conflux-Chain/confura/store"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestEarliestBlockMappingUsesSuppliedTransaction(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&epochBlockMap{}))

	mapStore := &epochBlockMapStore[store.EpochData]{baseStore: newBaseStore(db)}
	expectedRollback := errors.New("rollback test transaction")
	err = db.Transaction(func(dbTx *gorm.DB) error {
		require.NoError(t, dbTx.Create(&epochBlockMap{
			Epoch: 10, BnMin: 100, BnMax: 109, PivotHash: "pivot",
		}).Error)

		mapping, ok, err := mapStore.earliestBlockMapping(dbTx)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, uint64(10), mapping.Epoch)
		assert.Equal(t, uint64(100), mapping.BnMin)

		return expectedRollback
	})
	assert.ErrorIs(t, err, expectedRollback)

	_, ok, err := mapStore.EarliestBlockMapping()
	require.NoError(t, err)
	assert.False(t, ok)
}
