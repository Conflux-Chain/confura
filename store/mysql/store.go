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
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gorm.io/gorm"
)

const (
	// Logs table partition range (by ID) size
	logsTablePartitionRangeSize = uint64(20000000)
)

var (
	// Epoch data type mapping to mysql table name
	EpochDataTypeTableMap = map[store.EpochDataType]string{
		store.EpochBlock:       "blocks",
		store.EpochTransaction: "txs",
		store.EpochLog:         "logs",
	}
)

type mysqlStore struct {
	db       *gorm.DB
	minEpoch uint64 // minimum epoch number in database (historical data may be pruned)
	maxEpoch uint64 // maximum epoch number in database

	// Epoch range for block/transaction/log table in db
	epochRanges map[store.EpochDataType]*atomic.Value
	// Total rows for block/transaction/log table in db
	epochTotals map[store.EpochDataType]*uint64

	// Epoch range configurations for logs table partitions. It will be loaded from
	// database when store created.
	// Also be reminded logs table partitions must be created manually, and partitioned
	// by ID field with range size of 20,000,000 records.
	logsTablePartitionEpochRanges map[string]*atomic.Value
}

func mustNewStore(db *gorm.DB) *mysqlStore {
	mysqlStore := mysqlStore{
		db: db, minEpoch: math.MaxUint64, maxEpoch: math.MaxUint64,

		epochRanges: make(map[store.EpochDataType]*atomic.Value),
		epochTotals: make(map[store.EpochDataType]*uint64),

		logsTablePartitionEpochRanges: make(map[string]*atomic.Value),
	}

	for _, t := range store.OpEpochDataTypes {
		// Load epoch range
		minEpoch, maxEpoch, err := mysqlStore.loadEpochRange(t)
		if err != nil && !mysqlStore.IsRecordNotFound(err) {
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
		mysqlStore.epochRanges[t].Store(citypes.EpochRange{EpochFrom: minEpoch, EpochTo: maxEpoch})

		// Load epoch total
		total, err := mysqlStore.loadEpochTotal(t)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to load epoch total")
		}

		mysqlStore.epochTotals[t] = &total
	}

	// Load logs table partition information
	if err := mysqlStore.loadLogsTablePartitionInfo(); err != nil {
		logrus.WithError(err).Fatal("Failed to load logs table partition info")
	}

	logrus.WithFields(logrus.Fields{
		"globalEpochRange":       citypes.EpochRange{EpochFrom: mysqlStore.minEpoch, EpochTo: mysqlStore.maxEpoch},
		"epochRanges":            mysqlStore.dumpEpochRanges(),
		"epochTotals":            mysqlStore.dumpEpochTotals(),
		"logsTablePartitionInfo": mysqlStore.dumpLogsTablePartitionInfo(),
	}).Debug("New mysql store loaded")

	return &mysqlStore
}

func (ms *mysqlStore) dumpLogsTablePartitionInfo() string {
	strBuilder := &strings.Builder{}
	strBuilder.Grow(len(ms.logsTablePartitionEpochRanges) * 30)

	for p, er := range ms.logsTablePartitionEpochRanges {
		strBuilder.WriteString(fmt.Sprintf("%v:%v; ", p, er.Load().(citypes.EpochRange)))
	}

	return strBuilder.String()
}

func (ms *mysqlStore) dumpEpochRanges() string {
	strBuilder := &strings.Builder{}
	strBuilder.Grow(len(ms.epochRanges) * 30)

	for t, er := range ms.epochRanges {
		strBuilder.WriteString(fmt.Sprintf("%v:%v; ", EpochDataTypeTableMap[t], er.Load().(citypes.EpochRange)))
	}

	return strBuilder.String()
}

func (ms *mysqlStore) dumpEpochTotals() string {
	strBuilder := &strings.Builder{}
	strBuilder.Grow(len(ms.epochRanges) * 30)

	for t, v := range ms.epochTotals {
		strBuilder.WriteString(fmt.Sprintf("%v:%v; ", EpochDataTypeTableMap[t], *v))
	}

	return strBuilder.String()
}

func (ms *mysqlStore) IsRecordNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound) || err == store.ErrNotFound
}

// Load logs table partitioning information from database.
// Specifically, it will retrieve all logs table partitions, and then the
// epoch nunber ranges for each partition from the database.
// The epoch number ranges will be used to find the right partition(s) to
// boost the db query performance when conditioned with epoch
func (ms *mysqlStore) loadLogsTablePartitionInfo() error {
	sqlStatement := `SELECT PARTITION_NAME AS partiname FROM information_schema.partitions WHERE TABLE_SCHEMA='%v'
AND TABLE_NAME = 'logs' AND PARTITION_NAME IS NOT NULL ORDER BY PARTITION_ORDINAL_POSITION ASC`
	sqlStatement = fmt.Sprintf(sqlStatement, viper.GetString("store.mysql.database"))

	// Load all logs table partition names
	logsTblPartiNames := []string{}
	if err := ms.db.Raw(sqlStatement).Scan(&logsTblPartiNames).Error; err != nil {
		return err
	}

	// Load all logs table partition epoch number range
	for _, partiName := range logsTblPartiNames {
		sqlStatement := fmt.Sprintf("SELECT MIN(epoch) as minEpoch, MAX(epoch) as maxEpoch FROM logs PARTITION (%v)", partiName)

		row := ms.db.Raw(sqlStatement).Row()
		if err := row.Err(); err != nil {
			return err
		}

		var minEpoch, maxEpoch sql.NullInt64
		if err := row.Scan(&minEpoch, &maxEpoch); err != nil {
			return err
		}

		// Table partition not used yet, skip all next.
		if !minEpoch.Valid || !maxEpoch.Valid {
			break
		}

		ms.logsTablePartitionEpochRanges[partiName] = &atomic.Value{}
		ms.logsTablePartitionEpochRanges[partiName].Store(citypes.EpochRange{
			EpochFrom: uint64(minEpoch.Int64), EpochTo: uint64(maxEpoch.Int64),
		})
	}

	return nil
}

func (ms *mysqlStore) loadEpochRange(t store.EpochDataType) (uint64, uint64, error) {
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

func (ms *mysqlStore) loadEpochTotal(t store.EpochDataType) (uint64, error) {
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
	return ms.getEpochRange(store.EpochBlock)
}

func (ms *mysqlStore) GetTransactionEpochRange() (uint64, uint64, error) {
	return ms.getEpochRange(store.EpochTransaction)
}

func (ms *mysqlStore) GetLogEpochRange() (uint64, uint64, error) {
	return ms.getEpochRange(store.EpochLog)
}

func (ms *mysqlStore) GetGlobalEpochRange() (uint64, uint64, error) {
	return ms.getEpochRange(store.EpochDataNil)
}

func (ms *mysqlStore) getEpochRange(rt store.EpochDataType) (uint64, uint64, error) {
	var minEpoch, maxEpoch uint64

	if atmV, ok := ms.epochRanges[rt]; ok {
		// Get local epoch range for block/tx/log
		epochRange := atmV.Load().(citypes.EpochRange)
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
	return atomic.LoadUint64(ms.epochTotals[store.EpochBlock])
}

func (ms *mysqlStore) GetNumTransactions() uint64 {
	return atomic.LoadUint64(ms.epochTotals[store.EpochTransaction])
}

func (ms *mysqlStore) GetNumLogs() uint64 {
	return atomic.LoadUint64(ms.epochTotals[store.EpochLog])
}

func (ms *mysqlStore) GetLogs(filter store.LogFilter) (logs []types.Log, err error) {
	epochRange := ms.epochRanges[store.EpochLog].Load().(citypes.EpochRange)
	minEpoch, maxEpoch := epochRange.EpochFrom, epochRange.EpochTo

	logrus.WithFields(logrus.Fields{
		"epochRange":        epochRange,
		"logFilterMinEpoch": filter.EpochFrom,
		"logFilterMaxEpoch": filter.EpochTo,
	}).Debug("RPC getLogs requested from client")

	if filter.EpochFrom < minEpoch || filter.EpochTo > maxEpoch {
		return nil, gorm.ErrRecordNotFound
	}

	updater := metrics.NewTimerUpdaterByName("infura/store/mysql/getlogs")
	defer updater.Update()

	// Calcuate logs table partitions to get logs within the filter epoch range
	logsTblPartitions := ms.getLogsTablePartitionsForEpochRange(citypes.EpochRange{
		EpochFrom: filter.EpochFrom, EpochTo: filter.EpochTo,
	})

	return loadLogs(ms.db, filter, logsTblPartitions)
}

// Find the right logs table partition(s) for the specified epoch range.
// It will check all logs table paritions and return all the partitions of which
// epoch range are overlapped with the specified one.
func (ms *mysqlStore) getLogsTablePartitionsForEpochRange(epochRange citypes.EpochRange) []string {
	logsTblPartitions := make([]string, 0, 1)

	for partikey, atmV := range ms.logsTablePartitionEpochRanges {
		partiEpochRange := atmV.Load().(citypes.EpochRange)

		// Check if specified epoch range overlaps table partition epoch range
		if partiEpochRange.EpochFrom <= epochRange.EpochTo && partiEpochRange.EpochTo >= epochRange.EpochFrom {
			logsTblPartitions = append(logsTblPartitions, partikey)
		}
	}

	logrus.WithFields(logrus.Fields{
		"epochRange":        epochRange,
		"logsTblPartitions": logsTblPartitions,
	}).Debug("Logs table partitions for epoch range calculated")

	return logsTblPartitions
}

func (ms *mysqlStore) GetTransaction(txHash types.Hash) (*types.Transaction, error) {
	tx, err := loadTx(ms.db, txHash.String())
	if err != nil {
		return nil, err
	}

	var rpcTx types.Transaction
	util.MustUnmarshalRLP(tx.TxRawData, &rpcTx)

	return &rpcTx, nil
}

func (ms *mysqlStore) GetReceipt(txHash types.Hash) (*types.TransactionReceipt, error) {
	tx, err := loadTx(ms.db, txHash.String())
	if err != nil {
		return nil, err
	}

	var receipt types.TransactionReceipt
	util.MustUnmarshalRLP(tx.ReceiptRawData, &receipt)

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

	if len(result) == 0 { // no data in db since each epoch has at least 1 block (pivot block)
		return result, gorm.ErrRecordNotFound
	}

	return result, nil
}

func (ms *mysqlStore) GetBlockByEpoch(epochNumber uint64) (*types.Block, error) {
	// Cannot get tx from db in advance, since only executed txs saved in db
	return nil, store.ErrUnsupported
}

func (ms *mysqlStore) GetBlockSummaryByEpoch(epochNumber uint64) (*types.BlockSummary, error) {
	return loadBlock(ms.db, "epoch = ? AND pivot = true", epochNumber)
}

func (ms *mysqlStore) GetBlockByHash(blockHash types.Hash) (*types.Block, error) {
	return nil, store.ErrUnsupported
}

func (ms *mysqlStore) GetBlockSummaryByHash(blockHash types.Hash) (*types.BlockSummary, error) {
	hash := blockHash.String()
	return loadBlock(ms.db, "hash_id = ? AND hash = ?", util.GetShortIdOfHash(hash), hash)
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
			return errors.WithMessagef(store.ErrContinousEpochRequired, "expected epoch #%v, but #%v got", lastEpoch, data.Number)
		}
	}

	updater := metrics.NewTimerUpdaterByName("infura/duration/store/mysql/write")
	defer updater.Update()

	err := ms.execWithTx(func(dbTx *gorm.DB) (store.EpochDataOpAffects, error) {
		txOpHistory := store.EpochDataOpAffects{}

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
		for _, t := range store.OpEpochDataTypes {
			er := ms.epochRanges[t].Load().(citypes.EpochRange)
			er.EpochFrom = 0
			ms.epochRanges[t].Store(er)
		}
	}

	return err
}

func (ms *mysqlStore) execWithTx(txConsumeFunc func(dbTx *gorm.DB) (store.EpochDataOpAffects, error)) error {
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

func (ms *mysqlStore) putOneWithTx(dbTx *gorm.DB, data *store.EpochData) (store.EpochDataOpAffects, error) {
	opHistory := store.EpochDataOpAffects{}
	pivotIndex := len(data.Blocks) - 1

	for i, block := range data.Blocks {
		if err := dbTx.Create(newBlock(block, i == pivotIndex)).Error; err != nil {
			return opHistory, errors.WithMessagef(err, "Failed to write block #%v", block.Hash)
		}

		opHistory[store.EpochBlock]++

		// Containers to collect block trxs & trx logs for batch inserting
		trxs := make([]*transaction, 0)
		trxlogs := make([]*log, 0)

		for _, tx := range block.Transactions {
			receipt := data.Receipts[tx.Hash]

			// Skip transactions that unexecuted in block.
			// !!! Still need to check BlockHash and Status in case more than one transactions
			// of the same hash appeared in the same epoch.
			if receipt == nil || tx.BlockHash == nil || tx.Status == nil {
				continue
			}

			trxs = append(trxs, newTx(&tx, receipt))
			for _, log := range receipt.Logs {
				trxlogs = append(trxlogs, newLog(&log))
			}
		}

		// Batch insert block transactions
		if len(trxs) == 0 {
			continue
		}

		opHistory[store.EpochTransaction] += int64(len(trxs))

		if err := dbTx.Create(trxs).Error; err != nil {
			return opHistory, errors.WithMessagef(err, "Failed to batch write txs and receipts for block #%v", block.Hash)
		}

		// Batch insert block transaction event logs
		if len(trxlogs) == 0 {
			continue
		}

		opHistory[store.EpochLog] += int64(len(trxlogs))

		// According to statistics, some epoch block even has more than 2200 event logs (eg. epoch #13329688).
		// It would be better to insert limited event logs per time for good performance. With some testing, the
		// limited number set to 800 is ok for the momemt with slow sql statements hardly found in our log files.
		// TODO more benchmarks are needed for optimization.
		maxInsertLogs := 800
		rounds := len(trxlogs) / maxInsertLogs
		if len(trxlogs)%maxInsertLogs != 0 {
			rounds++
		}

		start := 0
		for i := 1; i <= rounds; i++ {
			end := util.MinInt(i*maxInsertLogs, len(trxlogs))
			if err := dbTx.Create(trxlogs[start:end]).Error; err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"start": start, "end": end, "blockHash": block.Hash,
				}).Error("Failed to insert transaction event logs part to database")

				return opHistory, errors.WithMessagef(err, "Failed to batch write event logs for block #%v", block.Hash)
			}
			start = end
		}

		// Collect table partitions to be updated with new epoch range
		updatedPartitions := make(map[string]bool, 2)

		for i := len(trxlogs) - 1; i >= 0; i-- {
			partiKey := fmt.Sprintf("logs%v", trxlogs[i].ID/logsTablePartitionRangeSize)
			if updatedPartitions[partiKey] {
				break
			}

			if atmV, ok := ms.logsTablePartitionEpochRanges[partiKey]; ok {
				epochRange := atmV.Load().(citypes.EpochRange)
				epochRange.EpochTo = util.MaxUint64(data.Number, epochRange.EpochTo)
				atmV.Store(epochRange)

				logrus.WithField("epochRange", epochRange).Debugf("Update epoch range for logs table partition %v", partiKey)
			} else {
				atmV = &atomic.Value{}
				epochRange := citypes.EpochRange{EpochFrom: data.Number, EpochTo: data.Number}

				atmV.Store(epochRange)
				ms.logsTablePartitionEpochRanges[partiKey] = atmV

				logrus.WithField("epochRange", epochRange).Debugf("Add epoch range for logs table partition %v", partiKey)
			}

			updatedPartitions[partiKey] = true
		}

		logrus.WithField("logsTblPartitionEpochRanges", ms.dumpLogsTablePartitionInfo()).Debug("Logs table partition epoch ranges info updated")
	}

	return opHistory, nil
}

func (ms *mysqlStore) Pop() error {
	maxEpoch := atomic.LoadUint64(&ms.maxEpoch)
	// Genesis block will never be popped
	if maxEpoch < 1 {
		return nil
	}

	if err := ms.remove(maxEpoch, maxEpoch, store.EpochRemoveAll); err != nil {
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

	if err := ms.remove(epochUntil, maxEpoch, store.EpochRemoveAll); err != nil {
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
	for _, t := range store.OpEpochDataTypes {
		epochRange := ms.epochRanges[t].Load().(citypes.EpochRange)

		epochRange.EpochTo = lastEpoch
		ms.epochRanges[t].Store(epochRange)
	}
}

func (ms *mysqlStore) remove(epochFrom, epochTo uint64, option store.EpochRemoveOption) error {
	updater := metrics.NewTimerUpdaterByName("infura/store/mysql/delete")
	defer updater.Update()

	err := ms.execWithTx(func(dbTx *gorm.DB) (store.EpochDataOpAffects, error) {
		opHistory := store.EpochDataOpAffects{}
		// Batch delete for better performance
		cond := fmt.Sprintf("epoch BETWEEN %v AND %v", epochFrom, epochTo)

		// Remove blocks
		if option&store.EpochRemoveBlock != 0 {
			db := dbTx.Delete(block{}, cond)
			if db.Error != nil {
				return opHistory, db.Error
			}

			opHistory[store.EpochBlock] -= db.RowsAffected
		}

		// Remove txs
		if option&store.EpochRemoveTransaction != 0 {
			db := dbTx.Delete(transaction{}, cond)
			if db.Error != nil {
				return opHistory, db.Error
			}

			opHistory[store.EpochTransaction] -= db.RowsAffected
		}

		// Remove logs
		if option&store.EpochRemoveLog != 0 {
			partitions := ms.getLogsTablePartitionsForEpochRange(citypes.EpochRange{EpochFrom: epochFrom, EpochTo: epochTo})
			if len(partitions) > 0 {
				dbTx = dbTx.Table(fmt.Sprintf("logs PARTITION (%v)", strings.Join(partitions, ",")))
			}

			db := dbTx.Delete(log{}, cond)
			if db.Error != nil {
				return opHistory, db.Error
			}

			opHistory[store.EpochLog] -= db.RowsAffected
		}

		return opHistory, nil
	})

	return err
}

func (ms *mysqlStore) DequeueBlocks(epochUntil uint64) error {
	return ms.dequeueEpochRangeData(store.EpochBlock, epochUntil)
}

func (ms *mysqlStore) DequeueTransactions(epochUntil uint64) error {
	return ms.dequeueEpochRangeData(store.EpochTransaction, epochUntil)
}

func (ms *mysqlStore) DequeueLogs(epochUntil uint64) error {
	return ms.dequeueEpochRangeData(store.EpochLog, epochUntil)
}

func (ms *mysqlStore) dequeueEpochRangeData(rt store.EpochDataType, epochUntil uint64) error {
	// Genesis block will never be dequeued
	epochUntil = util.MaxUint64(epochUntil, 1)

	// Get local epoch range for block/tx/log
	epochRange := ms.epochRanges[rt].Load().(citypes.EpochRange)
	if epochUntil < epochRange.EpochFrom {
		return nil
	}

	if err := ms.remove(epochRange.EpochFrom, epochUntil, store.EpochDataTypeRemoveOptionMap[rt]); err != nil {
		return err
	}

	// Update min epoch for local epoch range
	epochRange.EpochFrom = epochUntil + 1
	ms.epochRanges[rt].Store(epochRange)

	// Update global epoch ranges
	minEpoch := atomic.LoadUint64(&ms.minEpoch)
	for _, t := range store.OpEpochDataTypes {
		er := ms.epochRanges[t].Load().(citypes.EpochRange)
		minEpoch = util.MinUint64(minEpoch, er.EpochFrom)
	}
	atomic.StoreUint64(&ms.minEpoch, minEpoch)

	return nil
}

func (ms *mysqlStore) Close() error {
	if mysqlDb, err := ms.db.DB(); err != nil {
		return err
	} else {
		return mysqlDb.Close()
	}
}
