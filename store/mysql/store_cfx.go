package mysql

import (
	"context"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"gorm.io/gorm"
)

type CfxStore struct {
	*MysqlStore[*store.EpochData]
}

func NewCfxStore(db *gorm.DB, config *Config, filter store.ChainDataFilter) *CfxStore {
	return &CfxStore{
		MysqlStore: NewMysqlStore[*store.EpochData](db, config, filter),
	}
}

func (cs *CfxStore) GetTransaction(ctx context.Context, txHash types.Hash) (*types.Transaction, error) {
	var txn types.Transaction
	hashId := util.GetShortIdOfHash(txHash.String())

	err := decodeTransactionFromStore(cs.txStore, &txn, "hash_id = ? AND hash = ?", hashId, txHash)
	if err != nil {
		return nil, err
	}
	return &txn, nil
}

func (cs *CfxStore) GetReceipt(ctx context.Context, txHash types.Hash) (*types.TransactionReceipt, error) {
	var rcpt types.TransactionReceipt
	hashId := util.GetShortIdOfHash(txHash.String())

	err := decodeReceiptFromStore(cs.txStore, &rcpt, "hash_id = ? AND hash = ?", hashId, txHash)
	if err != nil {
		return nil, err
	}
	return &rcpt, nil
}

func (cs *CfxStore) GetBlocksByEpoch(ctx context.Context, epochNumber uint64) ([]types.Hash, error) {
	if cs.filter.IsBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	var result []types.Hash
	err := cs.blockStore.db.Model(&block{}).Where("epoch = ?", epochNumber).Pluck("hash", &result).Error
	if err != nil {
		return nil, err
	}

	if len(result) == 0 { // each epoch has at least 1 block (pivot block)
		return nil, gorm.ErrRecordNotFound
	}
	return result, nil
}

func (cs *CfxStore) GetBlockByEpoch(ctx context.Context, epochNumber uint64) (*types.Block, error) {
	// Note: Only block summary is supported.
	return nil, store.ErrUnsupported
}

func (cs *CfxStore) GetBlockSummaryByEpoch(ctx context.Context, epochNumber uint64) (*types.BlockSummary, error) {
	if cs.filter.IsBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	var bs types.BlockSummary
	err := decodeBlockFromStore(cs.blockStore, &bs, "epoch = ? AND pivot = true", epochNumber)
	if err != nil {
		return nil, err
	}
	return &bs, nil
}

func (cs *CfxStore) GetBlockByHash(ctx context.Context, blockHash types.Hash) (*types.Block, error) {
	// Note: Only block summary is supported.
	return nil, store.ErrUnsupported
}

func (cs *CfxStore) GetBlockSummaryByHash(ctx context.Context, blockHash types.Hash) (*types.BlockSummary, error) {
	if cs.filter.IsBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	var bs types.BlockSummary
	hash := blockHash.String()

	err := decodeBlockFromStore(cs.blockStore, &bs, "hash_id = ? AND hash = ?", util.GetShortIdOfHash(hash), hash)
	if err != nil {
		return nil, err
	}
	return &bs, nil
}

func (cs *CfxStore) GetBlockByBlockNumber(ctx context.Context, blockNumber uint64) (*types.Block, error) {
	// Note: Only block summary is supported.
	return nil, store.ErrUnsupported
}

func (cs *CfxStore) GetBlockSummaryByBlockNumber(ctx context.Context, blockNumber uint64) (*types.BlockSummary, error) {
	if cs.filter.IsBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	var bs types.BlockSummary
	err := decodeBlockFromStore(cs.blockStore, &bs, "block_number = ?", blockNumber)
	if err != nil {
		return nil, err
	}
	return &bs, nil
}
