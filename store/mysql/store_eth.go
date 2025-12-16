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

func (cs *EthStore) GetBlockByHash(blockHash common.Hash, includeTxs bool) (*types.Block, error) {
	if includeTxs || cs.filter.IsBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	var block types.Block
	hash := blockHash.String()

	err := decodeBlockFromStore(
		cs.blockStore, &block, "hash_id = ? AND hash = ?", util.GetShortIdOfHash(hash), hash,
	)
	if err != nil {
		return nil, err
	}

	return &block, nil
}

func (cs *EthStore) GetBlockByNumber(blockNum types.BlockNumber, includeTxs bool) (*types.Block, error) {
	if includeTxs || cs.filter.IsBlockDisabled() {
		return nil, store.ErrUnsupported
	}

	var block types.Block
	err := decodeBlockFromStore(cs.blockStore, &block, "block_number = ?", blockNum)
	if err != nil {
		return nil, err
	}

	return &block, nil
}

func (cs *EthStore) GetTransactionByHash(txHash common.Hash) (*types.TransactionDetail, error) {
	if cs.filter.IsTxnDisabled() {
		return nil, store.ErrUnsupported
	}

	var txn types.TransactionDetail
	err := decodeTransactionFromStore(
		cs.txStore, &txn, "hash_id = ? AND hash = ?", util.GetShortIdOfHash(txHash.String()), txHash,
	)
	if err != nil {
		return nil, err
	}
	return &txn, nil
}

func (cs *EthStore) GetTransactionReceipt(txHash common.Hash) (*types.Receipt, error) {
	if cs.filter.IsReceiptDisabled() {
		return nil, store.ErrUnsupported
	}

	var rcpt types.Receipt
	err := decodeReceiptFromStore(
		cs.txStore, &rcpt, "hash_id = ? AND hash = ?", util.GetShortIdOfHash(txHash.String()), txHash,
	)
	if err != nil {
		return nil, err
	}
	return &rcpt, nil
}
