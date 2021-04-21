package mysql

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync/atomic"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Epoch data type
type EpochDataType uint

const (
	EpochNil EpochDataType = iota
	EpochTransaction
	EpochLog
	EpochBlock
)

// Epoch data remove option
type EpochRemoveOption uint8

const (
	EpochRemoveAll         EpochRemoveOption = 0xff
	EpochRemoveBlock                         = 0x01 << 0
	EpochRemoveTransaction                   = 0x01 << 1
	EpochRemoveLog                           = 0x01 << 2
)

// EpochDataOpAffects to record num of changes for epoch data
type EpochDataOpAffects map[EpochDataType]int64

func (affects EpochDataOpAffects) String() string {
	sb := &strings.Builder{}
	sb.Grow(len(affects) * 30)

	for t, v := range affects {
		sb.WriteString(fmt.Sprintf("%v:%v; ", EpochDataTypeTableMap[t], v))
	}

	return sb.String()
}

var (
	errUnsupported            = errors.New("not supported")
	errContinousEpochRequired = errors.New("continous epoch required")

	OpEpochDataTypes      = []EpochDataType{EpochBlock, EpochTransaction, EpochLog}
	EpochDataTypeTableMap = map[EpochDataType]string{EpochBlock: "blocks", EpochTransaction: "txs", EpochLog: "logs"}

	epochDataTypeRemoveOptionMap = map[EpochDataType]EpochRemoveOption{
		EpochBlock: EpochRemoveBlock, EpochTransaction: EpochRemoveTransaction, EpochLog: EpochRemoveLog,
	}
)

type mysqlStore struct {
	db       *gorm.DB
	minEpoch uint64 // minimum epoch number in database (historical data may be pruned)
	maxEpoch uint64 // maximum epoch number in database

	// Epoch range for block/transaction/log table in db
	epochRanges map[EpochDataType]*atomic.Value
	// Total rows for block/transaction/log table in db
	epochTotals map[EpochDataType]*uint64
}

func mustNewStore(db *gorm.DB) *mysqlStore {
	mysqlStore := mysqlStore{
		db: db, minEpoch: math.MaxUint64, maxEpoch: math.MaxUint64,
		epochRanges: make(map[EpochDataType]*atomic.Value),
		epochTotals: make(map[EpochDataType]*uint64),
	}

	for _, t := range OpEpochDataTypes {
		// Load epoch range
		minEpoch, maxEpoch, err := mysqlStore.loadEpochRange(t)
		if err != nil && !gorm.IsRecordNotFoundError(err) {
			logrus.WithError(err).Fatal("Failed to load epoch range")
		} else if err == nil { // update global epoch range
			mysqlStore.minEpoch = util.MinUint64(mysqlStore.minEpoch, minEpoch)

			if mysqlStore.maxEpoch != math.MaxUint64 {
				mysqlStore.maxEpoch = util.MaxUint64(mysqlStore.maxEpoch, maxEpoch)
			} else { // initial setting
				mysqlStore.maxEpoch = maxEpoch
			}
		}

		mysqlStore.epochRanges[t] = &atomic.Value{}
		mysqlStore.epochRanges[t].Store(citypes.EpochRange{minEpoch, maxEpoch})

		// Load epoch total
		total, err := mysqlStore.loadEpochTotal(t)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to load epoch total")
		}

		mysqlStore.epochTotals[t] = &total
	}

	logrus.WithFields(logrus.Fields{
		"globalEpochRange": citypes.EpochRange{mysqlStore.minEpoch, mysqlStore.maxEpoch},
		"epochRanges":      mysqlStore.dumpEpochRanges(),
		"epochTotals":      mysqlStore.dumpEpochTotals(),
	}).Debug("New mysql store loaded")

	return &mysqlStore
}

func (ms *mysqlStore) dumpEpochRanges() string {
	sb := &strings.Builder{}
	sb.Grow(len(ms.epochRanges) * 30)

	for t, er := range ms.epochRanges {
		sb.WriteString(fmt.Sprintf("%v:%v; ", EpochDataTypeTableMap[t], er.Load().(citypes.EpochRange)))
	}

	return sb.String()
}

func (ms *mysqlStore) dumpEpochTotals() string {
	sb := &strings.Builder{}
	sb.Grow(len(ms.epochRanges) * 30)

	for t, v := range ms.epochTotals {
		sb.WriteString(fmt.Sprintf("%v:%v; ", EpochDataTypeTableMap[t], *v))
	}

	return sb.String()
}

func (ms *mysqlStore) IsRecordNotFound(err error) bool {
	return gorm.IsRecordNotFoundError(err)
}

func (ms *mysqlStore) loadEpochRange(t EpochDataType) (uint64, uint64, error) {
	sqlStatement := fmt.Sprintf("SELECT MIN(epoch) AS min_epoch, MAX(epoch) AS max_epoch FROM %v", EpochDataTypeTableMap[t])

	row := ms.db.Raw(sqlStatement).Row()
	if err := row.Err(); err != nil {
		return 0, 0, err
	}

	var minEpoch sql.NullInt64
	var maxEpoch sql.NullInt64

	if err := row.Scan(&minEpoch, &maxEpoch); err != nil {
		return 0, 0, err
	}

	if !minEpoch.Valid {
		return math.MaxUint64, math.MaxUint64, gorm.ErrRecordNotFound
	}

	return uint64(minEpoch.Int64), uint64(maxEpoch.Int64), nil
}

func (ms *mysqlStore) loadEpochTotal(t EpochDataType) (uint64, error) {
	sqlStatement := fmt.Sprintf("SELECT COUNT(*) AS total FROM %v", EpochDataTypeTableMap[t])

	row := ms.db.Raw(sqlStatement).Row()
	if err := row.Err(); err != nil {
		return 0, err
	}

	var total uint64
	err := row.Scan(&total)

	return total, err
}

func (ms *mysqlStore) GetBlockEpochRange() (uint64, uint64, error) {
	return ms.getEpochRange(EpochBlock)
}

func (ms *mysqlStore) GetTransactionEpochRange() (uint64, uint64, error) {
	return ms.getEpochRange(EpochTransaction)
}

func (ms *mysqlStore) GetLogEpochRange() (uint64, uint64, error) {
	return ms.getEpochRange(EpochLog)
}

func (ms *mysqlStore) GetGlobalEpochRange() (uint64, uint64, error) {
	return ms.getEpochRange(EpochNil)
}

func (ms *mysqlStore) getEpochRange(rt EpochDataType) (uint64, uint64, error) {
	var minEpoch, maxEpoch uint64

	if av, ok := ms.epochRanges[rt]; ok {
		// Get local epoch range for block/tx/log
		epochRange := av.Load().(citypes.EpochRange)
		minEpoch, maxEpoch = epochRange.EpochFrom, epochRange.EpochTo
	} else {
		// Default return as global epoch range
		minEpoch = atomic.LoadUint64(&ms.minEpoch)
		maxEpoch = atomic.LoadUint64(&ms.maxEpoch)
	}

	if maxEpoch == math.MaxUint64 {
		return 0, 0, gorm.ErrRecordNotFound
	}

	return minEpoch, maxEpoch, nil
}

func (ms *mysqlStore) GetNumBlocks() uint64 {
	return atomic.LoadUint64(ms.epochTotals[EpochBlock])
}

func (ms *mysqlStore) GetNumTransactions() uint64 {
	return atomic.LoadUint64(ms.epochTotals[EpochTransaction])
}

func (ms *mysqlStore) GetNumLogs() uint64 {
	return atomic.LoadUint64(ms.epochTotals[EpochLog])
}

func (ms *mysqlStore) GetLogs(filter store.LogFilter) (logs []types.Log, err error) {
	epochRange := ms.epochRanges[EpochLog].Load().(citypes.EpochRange)
	minEpoch, maxEpoch := epochRange.EpochFrom, epochRange.EpochTo

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

	err := ms.execWithTx(func(dbTx *gorm.DB) (EpochDataOpAffects, error) {
		txOpHistory := EpochDataOpAffects{}

		for _, data := range dataSlice {
			if opHistory, err := ms.putOneWithTx(dbTx, data); err != nil {
				return nil, err
			} else {
				// Merge operation history
				for k, v := range opHistory {
					txOpHistory[k] += v
				}
			}
		}

		return txOpHistory, nil
	})

	if err == nil {
		// Update max epoch range
		ms.updateMaxEpoch(lastEpoch)

		// Update global min epoch range if necessary (only when initial loading)
		if !atomic.CompareAndSwapUint64(&ms.minEpoch, math.MaxUint64, 0) {
			return nil
		}

		// Update all local min epoch ranges
		for _, t := range OpEpochDataTypes {
			er := ms.epochRanges[t].Load().(citypes.EpochRange)
			er.EpochFrom = 0
			ms.epochRanges[t].Store(er)
		}
	}

	return err
}

func (ms *mysqlStore) execWithTx(txConsumeFunc func(dbTx *gorm.DB) (EpochDataOpAffects, error)) error {
	dbTx := ms.db.Begin()
	if dbTx.Error != nil {
		return errors.WithMessage(dbTx.Error, "Failed to begin db tx")
	}

	opHistory, err := txConsumeFunc(dbTx)
	if err != nil {
		if rollbackErr := dbTx.Rollback().Error; rollbackErr != nil {
			logrus.WithError(rollbackErr).Error("Failed to rollback db tx")
		}

		return errors.WithMessage(err, "Failed to handle with db tx")
	}

	if err := dbTx.Commit().Error; err != nil {
		return errors.WithMessage(err, "Failed to commit db tx")
	}

	// Update epoch totals
	for k, v := range opHistory {
		switch {
		case v == 0:
			continue
		case v > 0: // increase
			atomic.AddUint64(ms.epochTotals[k], uint64(v))
		case v < 0: // decrease
			absV := -v

			for { // optimistic spin lock for thread safety
				oldTotal := atomic.LoadUint64(ms.epochTotals[k])
				newTotal := uint64(0)

				if oldTotal < uint64(absV) {
					logrus.Warn("DB store epoch totals decreased underflow")
				} else {
					newTotal = oldTotal - uint64(absV)
				}

				if atomic.CompareAndSwapUint64(ms.epochTotals[k], oldTotal, newTotal) {
					break
				}
			}
		}
	}

	logrus.WithFields(logrus.Fields{
		"opHistory":   opHistory,
		"epochTotals": ms.dumpEpochTotals(),
	}).Debug("Mysql store execWithTx after affect")

	return nil
}

func (ms *mysqlStore) putOneWithTx(dbTx *gorm.DB, data *store.EpochData) (EpochDataOpAffects, error) {
	opHistory := EpochDataOpAffects{}
	pivotIndex := len(data.Blocks) - 1

	for i, block := range data.Blocks {
		if err := dbTx.Create(newBlock(block, i == pivotIndex)).Error; err != nil {
			return opHistory, errors.WithMessage(err, "Failed to write block")
		}

		opHistory[EpochBlock]++

		for _, tx := range block.Transactions {
			receipt := data.Receipts[tx.Hash]

			// skip transactions that unexecuted in block
			if receipt == nil {
				continue
			}

			if err := dbTx.Create(newTx(&tx, receipt)).Error; err != nil {
				return opHistory, errors.WithMessage(err, "Failed to write tx and receipt")
			}

			opHistory[EpochTransaction]++

			for _, log := range receipt.Logs {
				if err := dbTx.Create(newLog(&log)).Error; err != nil {
					return opHistory, errors.WithMessage(err, "Failed to write event log")
				}

				opHistory[EpochLog]++
			}
		}
	}

	return opHistory, nil
}

func (ms *mysqlStore) Pop() error {
	maxEpoch := atomic.LoadUint64(&ms.maxEpoch)
	// Genesis block will never be popped
	if maxEpoch < 1 {
		return nil
	}

	if err := ms.remove(maxEpoch, maxEpoch, EpochRemoveAll); err != nil {
		return err
	}

	// Update max epoch
	ms.updateMaxEpoch(maxEpoch - 1)
	return nil
}

// Popn pops multiple epoch data from database.
func (ms *mysqlStore) Popn(epochUntil uint64) error {
	// Genesis block will never be popped
	epochUntil = util.MaxUint64(epochUntil, 1)

	maxEpoch := atomic.LoadUint64(&ms.maxEpoch)
	if epochUntil > maxEpoch {
		return nil
	}

	if err := ms.remove(epochUntil, maxEpoch, EpochRemoveAll); err != nil {
		return err
	}

	// Update max epoch
	ms.updateMaxEpoch(epochUntil - 1)
	return nil
}

func (ms *mysqlStore) updateMaxEpoch(lastEpoch uint64) {
	// Update global epoch range
	atomic.StoreUint64(&ms.maxEpoch, lastEpoch)

	// Update local epoch ranges
	for _, t := range OpEpochDataTypes {
		epochRange := ms.epochRanges[t].Load().(citypes.EpochRange)

		epochRange.EpochTo = lastEpoch
		ms.epochRanges[t].Store(epochRange)
	}
}

func (ms *mysqlStore) remove(epochFrom, epochTo uint64, option EpochRemoveOption) error {
	updater := metrics.NewTimerUpdaterByName("infura/store/mysql/delete")
	defer updater.Update()

	err := ms.execWithTx(func(dbTx *gorm.DB) (EpochDataOpAffects, error) {
		opHistory := EpochDataOpAffects{}
		// Batch delete for better performance
		cond := fmt.Sprintf("epoch BETWEEN %v AND %v", epochFrom, epochTo)

		// Remove blocks
		if option&EpochRemoveBlock != 0 {
			db := dbTx.Delete(block{}, cond)
			if db.Error != nil {
				return opHistory, db.Error
			}

			opHistory[EpochBlock] -= db.RowsAffected
		}

		// Remove txs
		if option&EpochRemoveTransaction != 0 {
			db := dbTx.Delete(transaction{}, cond)
			if db.Error != nil {
				return opHistory, db.Error
			}

			opHistory[EpochTransaction] -= db.RowsAffected
		}

		// Remove logs
		if option&EpochRemoveLog != 0 {
			db := dbTx.Delete(log{}, cond)
			if db.Error != nil {
				return opHistory, db.Error
			}

			opHistory[EpochLog] -= db.RowsAffected
		}

		return opHistory, nil
	})

	return err
}

func (ms *mysqlStore) DequeueBlocks(epochUntil uint64) error {
	return ms.dequeueEpochRangeData(EpochBlock, epochUntil)
}

func (ms *mysqlStore) DequeueTransactions(epochUntil uint64) error {
	return ms.dequeueEpochRangeData(EpochTransaction, epochUntil)
}

func (ms *mysqlStore) DequeueLogs(epochUntil uint64) error {
	return ms.dequeueEpochRangeData(EpochLog, epochUntil)
}

func (ms *mysqlStore) dequeueEpochRangeData(rt EpochDataType, epochUntil uint64) error {
	// Genesis block will never be dequeued
	epochUntil = util.MaxUint64(epochUntil, 1)

	// Get local epoch range for block/tx/log
	epochRange := ms.epochRanges[rt].Load().(citypes.EpochRange)
	if epochUntil < epochRange.EpochFrom {
		return nil
	}

	if err := ms.remove(epochRange.EpochFrom, epochUntil, epochDataTypeRemoveOptionMap[rt]); err != nil {
		return err
	}

	// Update min epoch for local epoch range
	epochRange.EpochFrom = epochUntil + 1
	ms.epochRanges[rt].Store(epochRange)

	// Update global epoch ranges
	minEpoch := atomic.LoadUint64(&ms.minEpoch)
	for _, t := range OpEpochDataTypes {
		er := ms.epochRanges[t].Load().(citypes.EpochRange)
		minEpoch = util.MinUint64(minEpoch, er.EpochFrom)
	}
	atomic.StoreUint64(&ms.minEpoch, minEpoch)

	return nil
}

func (ms *mysqlStore) Close() error {
	return ms.db.Close()
}
