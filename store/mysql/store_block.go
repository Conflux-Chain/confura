package mysql

import (
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common"
	"gorm.io/gorm"
)

const defaultBatchSizeBlockInsert = 500

type block struct {
	ID          uint64
	Epoch       uint64 `gorm:"not null;index"`
	BlockNumber uint64 `gorm:"not null;index"`
	HashId      uint64 `gorm:"not null;index"`
	Hash        string `gorm:"size:66;not null"`
	Pivot       bool   `gorm:"not null"`
	RawData     []byte `gorm:"type:MEDIUMBLOB;not null"`
	RawDataLen  uint64 `gorm:"not null"`
	Extra       []byte `gorm:"type:text"` // dynamic fields within json string for extention
}

func newBlock(data *types.Block, pivot bool, extra *store.BlockExtra) *block {
	block := &block{
		Epoch:       data.EpochNumber.ToInt().Uint64(),
		BlockNumber: data.BlockNumber.ToInt().Uint64(),
		Hash:        data.Hash.String(),
		Pivot:       pivot,
		RawData:     util.MustMarshalRLP(util.GetSummaryOfBlock(data)),
	}

	block.HashId = util.GetShortIdOfHash(block.Hash)
	block.RawDataLen = uint64(len(block.RawData))

	if extra != nil {
		// to save storage space, skip saving zero mix hash
		if util.IsZeroHash(extra.MixHash) {
			extra.MixHash = nil
		}

		block.Extra = util.MustMarshalJson(extra)
	}

	return block
}

func (block *block) parseBlockExtra() *store.BlockExtra {
	if len(block.Extra) > 0 {
		var extra store.BlockExtra
		util.MustUnmarshalJson(block.Extra, &extra)

		// To save storage space, we skip saving zero mixHash within extra field.
		// Here we restore the zero mixHash if necessary.
		if util.IsZeroHash(extra.MixHash) {
			extra.MixHash = &common.Hash{}
		}

		return &extra
	}

	return nil
}

type blockStore struct {
	db *gorm.DB
}

func newBlockStore(db *gorm.DB) *blockStore {
	return &blockStore{
		db: db,
	}
}

func (bs *blockStore) loadBlockSummary(whereClause string, args ...interface{}) (*store.BlockSummary, error) {
	var blk block
	if err := bs.db.Where(whereClause, args...).First(&blk).Error; err != nil {
		return nil, err
	}

	var summary types.BlockSummary
	util.MustUnmarshalRLP(blk.RawData, &summary)

	return &store.BlockSummary{
		CfxBlockSummary: &summary, Extra: blk.parseBlockExtra(),
	}, nil
}

func (bs *blockStore) GetBlocksByEpoch(epochNumber uint64) ([]types.Hash, error) {
	rows, err := bs.db.Raw("SELECT hash FROM blocks WHERE epoch = ?", epochNumber).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []types.Hash

	for rows.Next() {
		var hash string

		if err = rows.Scan(&hash); err != nil {
			return nil, err
		}

		result = append(result, types.Hash(hash))
	}

	if len(result) == 0 { // no data in db since each epoch has at least 1 block (pivot block)
		return result, gorm.ErrRecordNotFound
	}

	return result, nil
}

func (bs *blockStore) GetBlockByEpoch(epochNumber uint64) (*store.Block, error) {
	// TODO Cannot get tx from db in advance, since only executed txs saved in db
	return nil, store.ErrUnsupported
}

func (bs *blockStore) GetBlockSummaryByEpoch(epochNumber uint64) (*store.BlockSummary, error) {
	return bs.loadBlockSummary("epoch = ? AND pivot = true", epochNumber)
}

func (bs *blockStore) GetBlockByHash(blockHash types.Hash) (*store.Block, error) {
	return nil, store.ErrUnsupported
}

func (bs *blockStore) GetBlockSummaryByHash(blockHash types.Hash) (*store.BlockSummary, error) {
	hash := blockHash.String()
	return bs.loadBlockSummary("hash_id = ? AND hash = ?", util.GetShortIdOfHash(hash), hash)
}

func (bs *blockStore) GetBlockByBlockNumber(blockNumber uint64) (*store.Block, error) {
	return nil, store.ErrUnsupported
}

func (bs *blockStore) GetBlockSummaryByBlockNumber(blockNumber uint64) (*store.BlockSummary, error) {
	return bs.loadBlockSummary("block_number = ?", blockNumber)
}

// Pushn batch save epoch blocks into db store.
func (bs *blockStore) Pushn(dbTx *gorm.DB, dataSlice []*store.EpochData) error {
	var blocks []*block

	for _, data := range dataSlice {
		pivotIndex := len(data.Blocks) - 1

		for i, block := range data.Blocks {
			var blockExt *store.BlockExtra
			if i < len(data.BlockExts) {
				blockExt = data.BlockExts[i]
			}

			blocks = append(blocks, newBlock(block, i == pivotIndex, blockExt))
		}
	}

	if len(blocks) == 0 {
		return nil
	}

	return dbTx.CreateInBatches(blocks, defaultBatchSizeBlockInsert).Error
}
