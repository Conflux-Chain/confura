package mysql

import (
	"encoding/json"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type block struct {
	ID          uint64
	Epoch       uint64 `gorm:"not null;index"`
	BlockNumber uint64 `gorm:"not null;index"`
	HashId      uint64 `gorm:"not null;index"`
	Hash        string `gorm:"size:66;not null"`
	Pivot       bool   `gorm:"not null"`
	Extra       []byte `gorm:"type:text"` // json representation of the block
}

func newBlock(data store.BlockLike, pivot bool) (*block, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to json marshal block")
	}

	return &block{
		Epoch:       data.Epoch(),
		BlockNumber: data.Number(),
		Hash:        data.Hash(),
		HashId:      util.GetShortIdOfHash(data.Hash()),
		Pivot:       pivot,
		Extra:       bytes,
	}, nil
}

type blockStore[T store.ChainData] struct {
	db *gorm.DB
}

func newBlockStore[T store.ChainData](db *gorm.DB) *blockStore[T] {
	return &blockStore[T]{db: db}
}

func (bs *blockStore[T]) loadBlock(selects []string, whereClause string, args ...interface{}) (*block, error) {
	db := bs.db
	if len(selects) > 0 {
		db = db.Select(selects)
	}

	var blk block
	if err := db.Where(whereClause, args...).First(&blk).Error; err != nil {
		return nil, err
	}
	return &blk, nil
}

func decodeBlockFromStore[D store.ChainData, T any](
	bs *blockStore[D], result *T, whereClause string, args ...interface{}) error {
	block, err := bs.loadBlock([]string{"extra"}, whereClause, args...)
	if err != nil {
		return errors.WithMessage(err, "failed to load block")
	}

	if err := json.Unmarshal(block.Extra, result); err != nil {
		return errors.WithMessage(err, "failed to unmarshal block")
	}
	return nil
}

// Add batch save epoch blocks into db store.
func (bs *blockStore[T]) Add(dbTx *gorm.DB, dataSlice []T) error {
	var blocks []*block

	for _, data := range dataSlice {
		dblocks := data.ExtractBlocks()
		pivotIndex := len(dblocks) - 1

		for i, block := range dblocks {
			block, err := newBlock(block, i == pivotIndex)
			if err != nil {
				return errors.WithMessage(err, "failed to new block")
			}
			blocks = append(blocks, block)
		}
	}

	if len(blocks) == 0 {
		return nil
	}

	return dbTx.Create(blocks).Error
}

// Remove remove blocks of specific epoch range from db store.
func (bs *blockStore[T]) Remove(dbTx *gorm.DB, epochFrom, epochTo uint64) error {
	return dbTx.Where("epoch >= ? AND epoch <= ?", epochFrom, epochTo).Delete(&block{}).Error
}
