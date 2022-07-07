package mysql

import (
	"context"
	"math/big"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"gorm.io/gorm"
)

const defaultBatchSizeTxnInsert = 500

type transaction struct {
	ID                uint64
	Epoch             uint64 `gorm:"not null;index"`
	HashId            uint64 `gorm:"not null;index"` // as an index, number is better than long string
	Hash              string `gorm:"size:66;not null"`
	TxRawData         []byte `gorm:"type:MEDIUMBLOB"`
	TxRawDataLen      uint64 `gorm:"not null"`
	ReceiptRawData    []byte `gorm:"type:MEDIUMBLOB"`
	ReceiptRawDataLen uint64 `gorm:"not null"`
	NumReceiptLogs    int    `gorm:"not null"`
	Extra             []byte `gorm:"type:text"` // txn extention json field
	ReceiptExtra      []byte `gorm:"type:text"` // receipt extention json field
}

func (transaction) TableName() string {
	return "txs"
}

func newTx(
	tx *types.Transaction, receipt *types.TransactionReceipt, txExtra *store.TransactionExtra,
	rcptExtra *store.ReceiptExtra, skipTx, skipReceipt bool,
) *transaction {
	result := &transaction{
		Epoch: uint64(*receipt.EpochNumber),
		Hash:  tx.Hash.String(),
	}

	if !skipTx {
		result.TxRawData = util.MustMarshalRLP(tx)
	}

	if !skipReceipt {
		result.ReceiptRawData = util.MustMarshalRLP(receipt)
	}

	result.HashId = util.GetShortIdOfHash(result.Hash)
	result.TxRawDataLen = uint64(len(result.TxRawData))
	result.ReceiptRawDataLen = uint64(len(result.ReceiptRawData))
	result.NumReceiptLogs = len(receipt.Logs)

	if !skipTx && txExtra != nil {
		// no need to store block number since epoch number can be used instead
		txExtra.BlockNumber = nil
		result.Extra = util.MustMarshalJson(txExtra)
	}

	if !skipReceipt && rcptExtra != nil {
		result.ReceiptExtra = util.MustMarshalJson(rcptExtra)
	}

	return result
}

func (tx *transaction) parseTxExtra() *store.TransactionExtra {
	if len(tx.Extra) == 0 {
		return nil
	}

	var extra store.TransactionExtra
	util.MustUnmarshalJson(tx.Extra, &extra)

	// To save space, we don't save blockNumber within extra fields.
	// Here we restore the block number from epoch number.
	extra.BlockNumber = (*hexutil.Big)(big.NewInt(int64(tx.Epoch)))
	return &extra
}

func (tx *transaction) parseTxReceiptExtra() *store.ReceiptExtra {
	if len(tx.ReceiptExtra) == 0 {
		return nil
	}

	var extra store.ReceiptExtra
	util.MustUnmarshalJson(tx.ReceiptExtra, &extra)

	return &extra
}

type txStore struct {
	db *gorm.DB
}

func newTxStore(db *gorm.DB) *txStore {
	return &txStore{
		db: db,
	}
}

func (ts *txStore) loadTx(txHash types.Hash) (*transaction, error) {
	hashId := util.GetShortIdOfHash(txHash.String())

	var tx transaction
	if err := ts.db.Where("hash_id = ? AND hash = ?", hashId, txHash).First(&tx).Error; err != nil {
		return nil, err
	}

	return &tx, nil
}

func (ts *txStore) GetTransaction(ctx context.Context, txHash types.Hash) (*store.Transaction, error) {
	tx, err := ts.loadTx(txHash)
	if err != nil {
		return nil, err
	}

	var rpcTx types.Transaction
	util.MustUnmarshalRLP(tx.TxRawData, &rpcTx)

	return &store.Transaction{
		CfxTransaction: &rpcTx, Extra: tx.parseTxExtra(),
	}, nil
}

func (ts *txStore) GetReceipt(ctx context.Context, txHash types.Hash) (*store.TransactionReceipt, error) {
	tx, err := ts.loadTx(txHash)
	if err != nil {
		return nil, err
	}

	var receipt types.TransactionReceipt
	util.MustUnmarshalRLP(tx.ReceiptRawData, &receipt)

	ptrRcptExtra := tx.parseTxReceiptExtra()

	return &store.TransactionReceipt{
		CfxReceipt: &receipt, Extra: ptrRcptExtra,
	}, nil
}

// Add batch save epoch transactions into db store.
func (ts *txStore) Add(dbTx *gorm.DB, dataSlice []*store.EpochData, skipTx, skipRcpt bool) error {
	if skipTx && skipRcpt {
		return nil
	}

	// containers to collect transactions and receipts for batch inserting
	var txns []*transaction

	for _, data := range dataSlice {
		for i, block := range data.Blocks {
			var blockExt *store.BlockExtra
			if i < len(data.BlockExts) {
				blockExt = data.BlockExts[i]
			}

			for j, tx := range block.Transactions {
				receipt := data.Receipts[tx.Hash]

				// Skip transactions that unexecuted in block.
				// !!! Still need to check BlockHash and Status in case more than one transactions
				// of the same hash appeared in the same epoch.
				if receipt == nil || !util.IsTxExecutedInBlock(&tx) {
					continue
				}

				var txExt *store.TransactionExtra
				if blockExt != nil && j < len(blockExt.TxnExts) {
					txExt = blockExt.TxnExts[j]
				}

				var rcptExt *store.ReceiptExtra
				if len(data.ReceiptExts) > 0 {
					rcptExt = data.ReceiptExts[tx.Hash]
				}

				if !skipTx || !skipRcpt {
					txn := newTx(&tx, receipt, txExt, rcptExt, skipTx, skipRcpt)
					txns = append(txns, txn)
				}
			}
		}
	}

	if len(txns) == 0 {
		return nil
	}

	return dbTx.CreateInBatches(txns, defaultBatchSizeTxnInsert).Error
}

// Remove remove transactions of specific epoch range from db store.
func (ts *txStore) Remove(dbTx *gorm.DB, epochFrom, epochTo uint64) error {
	return dbTx.Where("epoch >= ? AND epoch <= ?", epochFrom, epochTo).Delete(&transaction{}).Error
}
