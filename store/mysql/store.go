package mysql

import (
	"database/sql"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	errUnsupported            = errors.New("not supported")
	errContinousEpochRequired = errors.New("continous epoch required")
)

type mysqlStore struct {
	db       *gorm.DB
	minEpoch uint64 // minimum epoch number in database (historical data may be pruned)
	maxEpoch uint64 // maximum epoch number in database
}

func mustNewStore(db *gorm.DB) *mysqlStore {
	store := mysqlStore{db, 0, 0}

	var err error
	store.minEpoch, store.maxEpoch, err = store.loadBlockEpochRange()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to load block epoch range")
	}

	return &store
}

func (ms *mysqlStore) IsRecordNotFound(err error) bool {
	return gorm.IsRecordNotFoundError(err)
}

func (ms *mysqlStore) loadBlockEpochRange() (uint64, uint64, error) {
	row := ms.db.Raw("SELECT MIN(epoch) min_epoch, MAX(epoch) max_epoch FROM blocks").Row()
	if err := row.Err(); err != nil {
		return 0, 0, err
	}

	var minEpoch sql.NullInt64
	var maxEpoch sql.NullInt64

	if err := row.Scan(&minEpoch, &maxEpoch); err != nil {
		return 0, 0, err
	}

	if !minEpoch.Valid {
		return math.MaxUint64, math.MaxUint64, nil
	}

	return uint64(minEpoch.Int64), uint64(maxEpoch.Int64), nil
}

func (ms *mysqlStore) GetBlockEpochRange() (uint64, uint64, error) {
	maxEpoch := atomic.LoadUint64(&ms.maxEpoch)

	if maxEpoch == math.MaxUint64 {
		return 0, 0, gorm.ErrRecordNotFound
	}

	minEpoch := atomic.LoadUint64(&ms.minEpoch)

	return minEpoch, maxEpoch, nil
}

func (ms *mysqlStore) GetLogs(filter store.LogFilter) ([]types.Log, error) {
	minEpoch := atomic.LoadUint64(&ms.minEpoch)
	maxEpoch := atomic.LoadUint64(&ms.maxEpoch)

	if filter.EpochFrom < minEpoch || filter.EpochTo > maxEpoch {
		return nil, gorm.ErrRecordNotFound
	}

	updater := metrics.NewTimerUpdaterByName("infura/store/mysql/getlogs")
	defer updater.Update()

	return loadLogs(ms.db, filter)
}

func (ms *mysqlStore) GetTransaction(txHash types.Hash) (*types.Transaction, error) {
	tx, err := loadTx(ms.db, txHash.String())
	if err != nil {
		return nil, err
	}

	var rpcTx types.Transaction
	mustUnmarshalRLP(tx.TxRawData, &rpcTx)

	return &rpcTx, nil
}

func (ms *mysqlStore) GetReceipt(txHash types.Hash) (*types.TransactionReceipt, error) {
	tx, err := loadTx(ms.db, txHash.String())
	if err != nil {
		return nil, err
	}

	var receipt types.TransactionReceipt
	mustUnmarshalRLP(tx.ReceiptRawData, &receipt)

	return &receipt, nil
}

func (ms *mysqlStore) GetBlocksByEpoch(epochNumber uint64) ([]types.Hash, error) {
	rows, err := ms.db.Raw("SELECT hash FROM blocks WHERE epoch = ?", epochNumber).Rows()
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

func (ms *mysqlStore) GetBlockByEpoch(epochNumber uint64) (*types.Block, error) {
	// Cannot get tx from db in advance, since only executed txs saved in db
	return nil, errUnsupported
}

func (ms *mysqlStore) GetBlockSummaryByEpoch(epochNumber uint64) (*types.BlockSummary, error) {
	return loadBlock(ms.db, "epoch = ? AND pivot = true", epochNumber)
}

func (ms *mysqlStore) GetBlockByHash(blockHash types.Hash) (*types.Block, error) {
	return nil, errUnsupported
}

func (ms *mysqlStore) GetBlockSummaryByHash(blockHash types.Hash) (*types.BlockSummary, error) {
	hash := blockHash.String()
	return loadBlock(ms.db, "hash_id = ? AND hash = ?", hash2ShortId(hash), hash)
}

func (ms *mysqlStore) Push(data *store.EpochData) error {
	return ms.Pushn([]*store.EpochData{data})
}

func (ms *mysqlStore) Pushn(dataSlice []*store.EpochData) error {
	if len(dataSlice) == 0 {
		return nil
	}

	// ensure continous epoch
	lastEpoch := atomic.LoadUint64(&ms.maxEpoch)
	for _, data := range dataSlice {
		lastEpoch++

		if data.Number != lastEpoch {
			return errContinousEpochRequired
		}
	}

	updater := metrics.NewTimerUpdaterByName("infura/store/mysql/write")
	defer updater.Update()

	err := ms.execWithTx(func(dbTx *gorm.DB) error {
		for _, data := range dataSlice {
			if err := ms.putOneWithTx(dbTx, data); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	atomic.StoreUint64(&ms.maxEpoch, lastEpoch)

	return nil
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

func (ms *mysqlStore) Pop() error {
	maxEpoch := atomic.LoadUint64(&ms.maxEpoch)
	return ms.remove(maxEpoch, maxEpoch)
}

func (ms *mysqlStore) Popn(epochUntil uint64) error {
	maxEpoch := atomic.LoadUint64(&ms.maxEpoch)
	if epochUntil > maxEpoch {
		return nil
	}

	return ms.remove(epochUntil, maxEpoch)
}

func (ms *mysqlStore) remove(epochFrom, epochTo uint64) error {
	updater := metrics.NewTimerUpdaterByName("infura/store/mysql/delete")
	defer updater.Update()

	err := ms.execWithTx(func(dbTx *gorm.DB) error {
		// Batch delete for better performance
		cond := fmt.Sprintf("epoch BETWEEN %v AND %v", epochFrom, epochTo)
		if err := dbTx.Delete(block{}, cond).Error; err != nil {
			return err
		}

		if err := dbTx.Delete(transaction{}, cond).Error; err != nil {
			return err
		}

		if err := dbTx.Delete(log{}, cond).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	// genesis block will never be reverted
	atomic.StoreUint64(&ms.maxEpoch, epochFrom-1)

	return nil
}

func (ms *mysqlStore) Close() error {
	return ms.db.Close()
}
