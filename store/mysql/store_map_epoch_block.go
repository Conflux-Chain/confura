package mysql

import (
	"database/sql"

	citypes "github.com/conflux-chain/conflux-infura/types"
	"gorm.io/gorm"
)

// epochBlockMap mapping data from epoch to relative block info.
// (such as block range and pivot block hash).
type epochBlockMap struct {
	ID uint64
	// epoch number
	Epoch uint64 `gorm:"index:unique;not null"`
	// min block number
	BnMin uint64 `gorm:"not null"`
	// max block number
	BnMax uint64 `gorm:"not null"`
	// pivot block hash used for parent hash checking
	PivotHash string `gorm:"size:66;not null"`
}

func (epochBlockMap) TableName() string {
	return "epoch_block_map"
}

// epochBlockMapStore used to get epoch to block map data.
type epochBlockMapStore struct {
	*baseStore
}

func newEochBlockMapStore(db *gorm.DB) *epochBlockMapStore {
	return &epochBlockMapStore{
		baseStore: newBaseStore(db),
	}
}

// epochRange returns the max epoch within the map store.
func (e2bms *epochBlockMapStore) maxEpoch() (uint64, bool, error) {
	var maxEpoch sql.NullInt64

	db := e2bms.db.Model(&epochBlockMap{}).Select("MAX(epoch)")
	if err := db.Find(&maxEpoch).Error; err != nil {
		return 0, false, err
	}

	if maxEpoch.Valid {
		return uint64(maxEpoch.Int64), true, nil
	}

	return 0, false, nil
}

// blockRange returns the spanning block range for the give epoch.
func (e2bms *epochBlockMapStore) blockRange(epoch uint64) (citypes.RangeUint64, bool, error) {
	var e2bmap epochBlockMap
	var bnr citypes.RangeUint64

	existed, err := e2bms.exists(&e2bmap, "epoch = ?", epoch)
	if err != nil {
		return bnr, false, err
	}

	bnr.From, bnr.To = e2bmap.BnMin, e2bmap.BnMax
	return bnr, existed, nil
}

// pivotHash returns the pivot hash of the given epoch.
func (e2bms *epochBlockMapStore) pivotHash(epoch uint64) (string, bool, error) {
	var e2bmap epochBlockMap

	existed, err := e2bms.exists(&e2bmap, "epoch = ?", epoch)
	if err != nil {
		return "", false, err
	}

	return e2bmap.PivotHash, existed, nil
}
