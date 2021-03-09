package mysql

import (
	"database/sql"
	"math/big"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type mysqlStore struct {
	db *gorm.DB
}

func newStore(db *gorm.DB) *mysqlStore {
	return &mysqlStore{db}
}

func (ms *mysqlStore) GetBlockEpochRange() (*big.Int, *big.Int, error) {
	row := ms.db.Raw("SELECT MIN(epoch) min_epoch, MAX(epoch) max_epoch FROM blocks").Row()
	if err := row.Err(); err != nil {
		return nil, nil, err
	}

	var minEpoch sql.NullInt64
	var maxEpoch sql.NullInt64

	if err := row.Scan(&minEpoch, &maxEpoch); err != nil {
		return nil, nil, err
	}

	if !minEpoch.Valid {
		return nil, nil, nil
	}

	return big.NewInt(minEpoch.Int64), big.NewInt(maxEpoch.Int64), nil
}

func (ms *mysqlStore) GetTransaction(txHash types.Hash) (*types.Transaction, error) {
	tx, err := loadTx(ms.db, txHash)
	if err != nil {
		return nil, err
	}

	var rpcTx types.Transaction
	mustUnmarshalJSON(tx.TxRawData, &rpcTx)

	return &rpcTx, nil
}

func (ms *mysqlStore) GetReceipt(txHash types.Hash) (*types.TransactionReceipt, error) {
	tx, err := loadTx(ms.db, txHash)
	if err != nil {
		return nil, err
	}

	var receipt types.TransactionReceipt
	mustUnmarshalJSON(tx.ReceiptRawData, &receipt)

	return &receipt, nil
}

func (ms *mysqlStore) GetBlocksByEpoch(epochNumber *big.Int) ([]types.Hash, error) {
	rows, err := ms.db.Raw("SELECT hash FROM blocks WHERE epoch = ?", epochNumber.Uint64()).Rows()
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

	return result, nil
}

func (ms *mysqlStore) GetBlockByEpoch(epochNumber *big.Int) (*types.Block, error) {
	return loadBlock(ms.db, "epoch = ? AND pivot = true", epochNumber.Uint64())
}

func (ms *mysqlStore) GetBlockSummaryByEpoch(epochNumber *big.Int) (*types.BlockSummary, error) {
	block, err := ms.GetBlockByEpoch(epochNumber)
	if err != nil {
		return nil, err
	}

	return convert2Summary(block), nil
}

func convert2Summary(block *types.Block) *types.BlockSummary {
	summary := types.BlockSummary{
		BlockHeader: block.BlockHeader,
	}

	for _, tx := range block.Transactions {
		summary.Transactions = append(summary.Transactions, tx.Hash)
	}

	return &summary
}

func (ms *mysqlStore) GetBlockByHash(blockHash types.Hash) (*types.Block, error) {
	return loadBlock(ms.db, "hash = ?", blockHash.String())
}

func (ms *mysqlStore) GetBlockSummaryByHash(blockHash types.Hash) (*types.BlockSummary, error) {
	block, err := ms.GetBlockByHash(blockHash)
	if err != nil {
		return nil, err
	}

	return convert2Summary(block), nil
}

func (ms *mysqlStore) PutEpochData(data *store.EpochData) error {
	return ms.PutEpochDataSlice([]*store.EpochData{data})
}

func (ms *mysqlStore) PutEpochDataSlice(dataSlice []*store.EpochData) error {
	updater := metrics.NewTimerUpdaterByName("store/mysql/write")
	defer updater.Update()

	return ms.execWithTx(func(dbTx *gorm.DB) error {
		for _, data := range dataSlice {
			if err := ms.putOneWithTx(dbTx, data); err != nil {
				return err
			}
		}

		return nil
	})
}

func (ms *mysqlStore) execWithTx(txConsumeFunc func(dbTx *gorm.DB) error) error {
	dbTx := ms.db.Begin()
	if dbTx.Error != nil {
		return errors.WithMessage(dbTx.Error, "Failed to begin db tx")
	}

	if err := txConsumeFunc(dbTx); err != nil {
		if rollbackErr := dbTx.Rollback().Error; rollbackErr != nil {
			logrus.WithError(rollbackErr).Error("Failed to rollback db tx")
		}

		return errors.WithMessage(err, "Failed to handle with db tx")
	}

	if err := dbTx.Commit().Error; err != nil {
		return errors.WithMessage(err, "Failed commit db tx")
	}

	return nil
}

func (ms *mysqlStore) putOneWithTx(dbTx *gorm.DB, data *store.EpochData) error {
	// TODO remove in case of pivot chain switched, and deeply reverted.
	// E.g. latest confirmed epoch is reverted

	pivotIndex := len(data.Blocks) - 1

	for i, block := range data.Blocks {
		if err := dbTx.Create(newBlock(block, i == pivotIndex)).Error; err != nil {
			return errors.WithMessage(err, "Failed to write block")
		}

		for _, tx := range block.Transactions {
			receipt := data.Receipts[tx.Hash]

			// skip transactions that unexecuted in block
			if receipt == nil {
				continue
			}

			if err := dbTx.Create(newTx(&tx, receipt)).Error; err != nil {
				return errors.WithMessage(err, "Failed to write tx and receipt")
			}

			for _, log := range receipt.Logs {
				if err := dbTx.Create(newLog(&log)).Error; err != nil {
					return errors.WithMessage(err, "Failed to write event log")
				}
			}
		}
	}

	return nil
}

func (ms *mysqlStore) Remove(epochFrom, epochTo *big.Int, includeTxs, includeLogs bool) error {
	updater := metrics.NewTimerUpdaterByName("store/mysql/delete")
	defer updater.Update()

	return ms.execWithTx(func(dbTx *gorm.DB) error {
		for i := epochFrom.Uint64(); i <= epochTo.Uint64(); i++ {
			if err := dbTx.Delete(block{}, "epoch = ?", i).Error; err != nil {
				return err
			}

			if includeTxs {
				if err := dbTx.Delete(transaction{}, "epoch = ?", i).Error; err != nil {
					return err
				}
			}

			if includeLogs {
				if err := dbTx.Delete(log{}, "epoch = ?", i).Error; err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (ms *mysqlStore) Close() error {
	return ms.db.Close()
}
