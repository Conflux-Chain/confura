package mysql

import (
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
	"gorm.io/gorm"
)

type EthStore struct {
	*MysqlStore[*store.EthData]
}

func NewEthStore(db *gorm.DB, config *Config, filter store.ChainDataFilter) *EthStore {
	return &EthStore{
		MysqlStore: NewMysqlStore[*store.EthData](db, config, filter),
	}
}

func (es *EthStore) GetBlockByHash(blockHash common.Hash, includeTxs bool) (*types.Block, error) {
	if includeTxs || es.filter.IsBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	var block types.Block
	hash := blockHash.String()

	err := decodeBlockFromStore(
		es.blockStore, &block, "hash_id = ? AND hash = ?", util.GetShortIdOfHash(hash), hash,
	)
	if err != nil {
		return nil, err
	}

	return &block, nil
}

func (es *EthStore) GetBlockByNumber(blockNum types.BlockNumber, includeTxs bool) (*types.Block, error) {
	if includeTxs || es.filter.IsBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	var block types.Block
	err := decodeBlockFromStore(es.blockStore, &block, "block_number = ?", blockNum)
	if err != nil {
		return nil, err
	}

	return &block, nil
}

func (es *EthStore) GetTransactionByHash(txHash common.Hash) (*types.TransactionDetail, error) {
	if es.filter.IsTxnDisabled() {
		return nil, store.ErrUnsupported
	}

	var txn types.TransactionDetail
	err := decodeTransactionFromStore(
		es.txStore, &txn, "hash_id = ? AND hash = ?", util.GetShortIdOfHash(txHash.String()), txHash,
	)
	if err != nil {
		return nil, err
	}
	return &txn, nil
}

func (es *EthStore) GetTransactionReceipt(txHash common.Hash) (*types.Receipt, error) {
	if es.filter.IsReceiptDisabled() {
		return nil, store.ErrUnsupported
	}

	var rcpt types.Receipt
	err := decodeReceiptFromStore(
		es.txStore, &rcpt, "hash_id = ? AND hash = ?", util.GetShortIdOfHash(txHash.String()), txHash,
	)
	if err != nil {
		return nil, err
	}
	return &rcpt, nil
}
