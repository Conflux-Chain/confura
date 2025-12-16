package mysql

import (
	"encoding/json"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type transaction struct {
	ID             uint64
	Epoch          uint64 `gorm:"not null;index"`
	HashId         uint64 `gorm:"not null;index"` // as an index, number is better than long string
	Hash           string `gorm:"size:66;not null"`
	NumReceiptLogs int    `gorm:"not null"`
	Extra          []byte `gorm:"type:text"` // json representation for transaction
	ReceiptExtra   []byte `gorm:"type:text"` // json representation for transaction receipt
}

func (transaction) TableName() string {
	return "txs"
}

func newTx[T store.ChainData](
	epochNumber uint64,
	tx store.TransactionLike,
	receipt store.ReceiptLike,
	skipTx, skipReceipt bool,
) (*transaction, error) {
	result := &transaction{
		Epoch: epochNumber,
		Hash:  tx.Hash(),
	}

	if !skipTx {
		bytes, err := json.Marshal(tx)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to json marshal transaction")
		}
		result.Extra = bytes
	}

	if !skipReceipt {
		bytes, err := json.Marshal(receipt)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to json marshal receipt")
		}
		result.ReceiptExtra = bytes
	}

	result.HashId = util.GetShortIdOfHash(result.Hash)
	result.NumReceiptLogs = len(receipt.Logs())

	return result, nil
}

type txStore[T store.ChainData] struct {
	db *gorm.DB
}

func newTxStore[T store.ChainData](db *gorm.DB) *txStore[T] {
	return &txStore[T]{db: db}
}

func (ts *txStore[T]) loadTx(selects []string, whereClause string, args ...interface{}) (*transaction, error) {
	db := ts.db
	if len(selects) > 0 {
		db = db.Select(selects)
	}

	var tx transaction
	if err := db.Where(whereClause, args...).First(&tx).Error; err != nil {
		return nil, err
	}
	return &tx, nil
}

func decodeTransactionFromStore[D store.ChainData, T any](
	ts *txStore[D], result *T, whereClause string, args ...interface{}) error {
	tx, err := ts.loadTx([]string{"extra"}, whereClause, args...)
	if err != nil {
		return errors.WithMessage(err, "failed to load transaction")
	}

	if err := json.Unmarshal(tx.Extra, &result); err != nil {
		return errors.WithMessage(err, "failed to unmarshal transaction")
	}
	return nil
}

func decodeReceiptFromStore[D store.ChainData, T any](
	ts *txStore[D], result *T, whereClause string, args ...interface{}) error {
	tx, err := ts.loadTx([]string{"receipt_extra"}, whereClause, args...)
	if err != nil {
		return errors.WithMessage(err, "failed to load transaction")
	}

	if err := json.Unmarshal(tx.ReceiptExtra, &result); err != nil {
		return errors.WithMessage(err, "failed to unmarshal transaction receipt")
	}
	return nil
}

// Add batch save epoch transactions into db store.
func (ts *txStore[T]) Add(dbTx *gorm.DB, dataSlice []T, skipTx, skipRcpt bool) error {
	if skipTx && skipRcpt {
		return nil
	}

	// containers to collect transactions and receipts for batch inserting
	var txns []*transaction

	for _, data := range dataSlice {
		for _, block := range data.ExtractBlocks() {
			receipts := data.ExtractReceipts()
			for _, tx := range block.Transactions() {
				receipt := receipts[tx.Hash()]

				// Skip transactions that unexecuted in block.
				// !!! Still need to check BlockHash and Status in case more than one transactions
				// of the same hash appeared in the same epoch.
				if receipt == nil || !tx.Executed() {
					continue
				}

				if !skipTx || !skipRcpt {
					txn, err := newTx[T](data.Number(), tx, receipt, skipTx, skipRcpt)
					if err != nil {
						return errors.WithMessage(err, "failed to new transaction")
					}
					txns = append(txns, txn)
				}
			}
		}
	}

	if len(txns) == 0 {
		return nil
	}

	return dbTx.Create(txns).Error
}

// Remove remove transactions of specific epoch range from db store.
func (ts *txStore[T]) Remove(dbTx *gorm.DB, epochFrom, epochTo uint64) error {
	return dbTx.Where("epoch >= ? AND epoch <= ?", epochFrom, epochTo).Delete(&transaction{}).Error
}
