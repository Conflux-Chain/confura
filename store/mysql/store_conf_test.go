package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCreateOrUpdateReorgVersionUsesTransaction(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&conf{}))

	store := newConfStore(db)
	dbTx := db.Begin()
	require.NoError(t, dbTx.Error)
	require.NoError(t, store.createOrUpdateReorgVersion(dbTx))
	require.NoError(t, dbTx.Rollback().Error)

	version, err := store.GetReorgVersion()
	require.NoError(t, err)
	assert.Zero(t, version)
}

func TestCreateOrUpdateReorgVersionAtomicallyIncrements(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&conf{}))

	store := newConfStore(db)
	require.NoError(t, store.createOrUpdateReorgVersion(db))
	require.NoError(t, store.createOrUpdateReorgVersion(db))

	version, err := store.GetReorgVersion()
	require.NoError(t, err)
	assert.Equal(t, 2, version)
}
