package mysql

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"

	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gorm.io/gorm"
)

var (
	_ store.Store = (*mysqlStore)(nil) // ensure mysqlStore implements Store interface

	// Epoch data type mapping to mysql table name
	EpochDataTypeTableMap = map[store.EpochDataType]string{
		store.EpochBlock:       "blocks",
		store.EpochTransaction: "txs",
		store.EpochLog:         "logs",
	}

	// Min number of epochs to be left pruned, which is used to check if an epoch pruned
	// or not against some epoch range.
	minNumEpochsLeftToBePruned uint64
)

func init() {
	maxPruneEpochs := viper.GetInt("prune.db.maxEpochs") * 2
	minNumEpochsLeftToBePruned = util.MaxUint64(uint64(maxPruneEpochs), 10)
}

type StoreOption struct {
	// Whether to calibrate epoch statistics by running MySQL OLAP if needed. This is necessary to
	// preload epoch statistics before sync. It's not necessary for rpc service since this operation
	// can be heavy and time consumming.
	CalibrateEpochStats bool
	Disabler            store.StoreDisabler
}

type mysqlEpochDataOpAffects struct {
	*store.EpochDataOpAffects
	// Value set to update new epoch range for logs partition
	logsPartEpochRangeRealSets map[string][2]*uint64 // partition name => new epoch range (no set if nil)
	logsPartIndexSets          []uint64              // indexes of partitions to be updated
}

func newMysqlEpochDataOpAffects(sea *store.EpochDataOpAffects) *mysqlEpochDataOpAffects {
	return &mysqlEpochDataOpAffects{
		EpochDataOpAffects:         sea,
		logsPartEpochRangeRealSets: make(map[string][2]*uint64),
	}
}

type mysqlStore struct {
	*baseStore
	logPartitioner
	*txStore
	*blockStore
	*logStore
	*confStore
	*UserStore
	*ContractStore

	db     *gorm.DB
	config *Config

	minEpoch uint64 // minimum epoch number in database (historical data may be pruned)
	maxEpoch uint64 // maximum epoch number in database

	// Epoch range for block/transaction/log table in db
	epochRanges map[store.EpochDataType]*citypes.EpochRange
	// Total rows for block/transaction/log table in db
	epochTotals map[store.EpochDataType]*uint64

	maxUsedLogsTblPartIdx uint64 // the maximum used partition index for logs table
	minUsedLogsTblPartIdx uint64 // the minimum used partition index for logs table

	disabler store.StoreDisabler // store chaindata disabler
}

func mustNewStore(db *gorm.DB, config *Config, option StoreOption) *mysqlStore {
	ms := mysqlStore{
		baseStore:     newBaseStore(db),
		txStore:       newTxStore(db),
		blockStore:    newBlockStore(db),
		logStore:      newLogStore(db),
		confStore:     newConfStore(db),
		UserStore:     newUserStore(db),
		ContractStore: NewContractStore(db),
		db:            db,
		config:        config,
		minEpoch:      citypes.EpochNumberNil,
		maxEpoch:      citypes.EpochNumberNil,
		epochRanges:   make(map[store.EpochDataType]*citypes.EpochRange),
		epochTotals:   make(map[store.EpochDataType]*uint64),
		disabler:      option.Disabler,
	}

	if option.CalibrateEpochStats {
		if err := ms.calibrateEpochStats(); err != nil {
			logrus.WithError(err).Fatal("Failed to calibrate epoch statistics")
		}

		logrus.WithFields(logrus.Fields{
			"globalEpochRange":         citypes.EpochRange{EpochFrom: ms.minEpoch, EpochTo: ms.maxEpoch},
			"epochRanges":              ms.dumpEpochRanges(),
			"epochTotals":              ms.dumpEpochTotals(),
			"usedLogsTblPartitionIdxs": citypes.EpochRange{EpochFrom: ms.minUsedLogsTblPartIdx, EpochTo: ms.maxUsedLogsTblPartIdx},
		}).Debug("New mysql store loaded with epoch stats")
	}

	return &ms
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

func (ms *mysqlStore) GetNumBlocks() uint64 {
	return atomic.LoadUint64(ms.epochTotals[store.EpochBlock])
}

func (ms *mysqlStore) GetNumTransactions() uint64 {
	return atomic.LoadUint64(ms.epochTotals[store.EpochTransaction])
}

func (ms *mysqlStore) GetNumLogs() uint64 {
	return atomic.LoadUint64(ms.epochTotals[store.EpochLog])
}

func (ms *mysqlStore) Push(data *store.EpochData) error {
	return ms.Pushn([]*store.EpochData{data})
}

func (ms *mysqlStore) Pushn(dataSlice []*store.EpochData) error {
	if len(dataSlice) == 0 {
		return nil
	}

	lastEpoch := atomic.LoadUint64(&ms.maxEpoch)
	pushFromEpoch := lastEpoch + 1
	insertLogs := false // if need to insert logs

	for _, data := range dataSlice {
		if lastEpoch == citypes.EpochNumberNil { // initial loading?
			lastEpoch = data.Number
			pushFromEpoch = data.Number
		} else {
			lastEpoch++
		}

		if data.Number != lastEpoch { // ensure continous epoch
			return errors.WithMessagef(
				store.ErrContinousEpochRequired,
				"expected epoch #%v, but #%v got", lastEpoch, data.Number,
			)
		}

		if ms.disabler.IsChainLogDisabled() || insertLogs || len(data.Receipts) == 0 {
			continue
		}

		for _, rcpt := range data.Receipts {
			if len(rcpt.Logs) > 0 {
				insertLogs = true
				break
			}
		}
	}

	updater := metrics.NewTimerUpdaterByName("infura/duration/store/mysql/write")
	defer updater.Update()

	// Disabled during development
	if ms.config.AddressIndexedLogEnabled {
		newAdded, err := ms.AddContractByEpochData(dataSlice...)
		if err != nil {
			return errors.WithMessage(err, "Failed to add contracts for specified epoch data slice")
		}

		// Note, even if failed to insert event logs afterward, no need to rollback the inserted contract records.
		if newAdded > 0 {
			logrus.WithField("count", newAdded).Debug("Succeeded to add new contract into database")
		}
	}

	opAffects := store.NewEpochDataOpAffects(store.EpochOpPush, pushFromEpoch, lastEpoch)
	txOpAffects := newMysqlEpochDataOpAffects(opAffects)
	insertLogsIDSpan := [2]uint64{citypes.EpochNumberNil, 0}

	err := ms.execWithTx(func(dbTx *gorm.DB) (*mysqlEpochDataOpAffects, error) {
		insertBeforeLogsPartEpochRanges, err := map[string]citypes.EpochRange{}, error(nil)
		if insertLogs { // get relative epoch ranges for logs table partitions before logs insert for late diff
			insertBeforeLogsPartEpochRanges, err = ms.loadLikelyActionLogsPartEpochRangesTx(dbTx, store.EpochOpPush)
			if err != nil {
				return txOpAffects, errors.WithMessage(err, "failed to load logs partitions epoch ranges before push")
			}
		}

		for _, data := range dataSlice {
			idSpan, opHistory, err := ms.putOneWithTx(dbTx, data)
			if err != nil {
				return nil, err
			}

			// merge operation history
			txOpAffects.Merge(opHistory)

			if insertLogs {
				// update insert logs id span
				insertLogsIDSpan[0] = util.MinUint64(insertLogsIDSpan[0], idSpan[0])
				insertLogsIDSpan[1] = util.MaxUint64(insertLogsIDSpan[1], idSpan[1])
			}
		}

		// recalculate epoch ranges of logs table partitions for inserted logs
		if insertLogs && insertLogsIDSpan[0] <= insertLogsIDSpan[1] {
			idxStart := ms.getLogsPartitionIdxFromId(insertLogsIDSpan[0])
			idxEnd := ms.getLogsPartitionIdxFromId(insertLogsIDSpan[1])

			for idx := idxStart; idx <= idxEnd; idx++ {
				partition := ms.getLogsPartitionNameByIdx(idx)
				afterER, err := ms.loadLogsTblPartitionEpochRanges(dbTx, partition)
				if err != nil {
					return txOpAffects, errors.WithMessage(err, "failed to load logs partitions epoch ranges after push")
				}

				beforeER, ok := insertBeforeLogsPartEpochRanges[partition]
				if !ok {
					logrus.WithField("partition", partition).Error("Unable to match epoch range for logs parition before push")
					return txOpAffects, errors.Errorf("unable to match epoch ranges for logs partition %v before push", partition)
				}

				txOpAffects.logsPartEpochRangeRealSets[partition] = ms.diffLogsPartitionEpochRangeForRealSet(beforeER, afterER)
			}
		}

		return txOpAffects, nil
	})

	return err
}

func (ms *mysqlStore) Pop() error {
	maxEpoch := atomic.LoadUint64(&ms.maxEpoch)
	return ms.Popn(maxEpoch)
}

// Popn pops multiple epoch data from database.
func (ms *mysqlStore) Popn(epochUntil uint64) error {
	// Genesis block will never be popped
	epochUntil = util.MaxUint64(epochUntil, 1)

	maxEpoch := atomic.LoadUint64(&ms.maxEpoch)
	if epochUntil > maxEpoch {
		return nil
	}

	opAffects := store.NewEpochDataOpAffects(store.EpochOpPop, epochUntil)
	txOpAffects := newMysqlEpochDataOpAffects(opAffects)
	err := ms.remove(epochUntil, maxEpoch, store.EpochRemoveAll, func() *mysqlEpochDataOpAffects {
		return txOpAffects
	})

	logrus.WithFields(logrus.Fields{
		"epochUntil": epochUntil, "stackMaxEpoch": maxEpoch,
		"epochOpAffects": txOpAffects.EpochDataOpAffects,
	}).WithError(err).Info("Epoch data popped out from db store")

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

func (ms *mysqlStore) execWithTx(txConsumeFunc func(dbTx *gorm.DB) (*mysqlEpochDataOpAffects, error)) error {
	dbTx := ms.db.Begin()
	if dbTx.Error != nil {
		return errors.WithMessage(dbTx.Error, "Failed to begin db tx")
	}

	rollback := func(err error) error {
		if rollbackErr := dbTx.Rollback().Error; rollbackErr != nil {
			logrus.WithError(rollbackErr).Error("Failed to rollback db tx")
		}
		return errors.WithMessage(err, "Failed to handle with db tx")
	}

	opAffects, err := txConsumeFunc(dbTx)
	if err != nil {
		return rollback(err)
	}

	if ms.updateEpochStatsWithTx(dbTx, opAffects) != nil {
		return rollback(errors.WithMessage(err, "Failed to update epoch stats"))
	}

	if err := dbTx.Commit().Error; err != nil {
		return errors.WithMessage(err, "Failed to commit db tx")
	}

	ms.updateEpochStats(opAffects)

	return nil
}

func (ms *mysqlStore) putOneWithTx(dbTx *gorm.DB, data *store.EpochData) ([2]uint64, store.EpochDataOpNumAlters, error) {
	opHistory := store.EpochDataOpNumAlters{}
	insertLogIdSpan := [2]uint64{citypes.EpochNumberNil, 0}

	pivotIndex := len(data.Blocks) - 1
	for i, block := range data.Blocks {
		var blockExt *store.BlockExtra
		if i < len(data.BlockExts) {
			blockExt = data.BlockExts[i]
		}

		if !ms.disabler.IsChainBlockDisabled() {
			if err := dbTx.Create(newBlock(block, i == pivotIndex, blockExt)).Error; err != nil {
				return insertLogIdSpan, opHistory, errors.WithMessagef(err, "failed to write block #%v", block.Hash)
			}

			opHistory[store.EpochBlock]++
		}

		// Containers to collect block trxs & trx logs for batch inserting
		trxs := make([]*transaction, 0)
		trxlogs := make([]*log, 0)

		for j, tx := range block.Transactions {
			receipt := data.Receipts[tx.Hash]

			// Skip transactions that unexecuted in block.
			// !!! Still need to check BlockHash and Status in case more than one transactions
			// of the same hash appeared in the same epoch.
			if receipt == nil || tx.BlockHash == nil || tx.Status == nil {
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

			skipTx := ms.disabler.IsChainTxnDisabled()
			skipRcpt := ms.disabler.IsChainReceiptDisabled()
			if !skipTx || !skipRcpt {
				txn := newTx(&tx, receipt, txExt, rcptExt, skipTx, skipRcpt)
				trxs = append(trxs, txn)
			}

			if !ms.disabler.IsChainLogDisabled() {
				for k, log := range receipt.Logs {
					blockNum := block.BlockNumber.ToInt().Uint64()

					var logExt *store.LogExtra
					if rcptExt != nil && k < len(rcptExt.LogExts) {
						logExt = rcptExt.LogExts[k]
					}

					trxlogs = append(trxlogs, newLog(blockNum, &log, logExt))
				}
			}
		}

		// Batch insert block transactions
		if len(trxs) > 0 {
			if err := dbTx.Create(trxs).Error; err != nil {
				return insertLogIdSpan, opHistory, errors.WithMessagef(
					err, "Failed to batch write txs and receipts for block #%v", block.Hash,
				)
			}

			opHistory[store.EpochTransaction] += int64(len(trxs))
		}

		// Batch insert block transaction event logs
		if len(trxlogs) == 0 {
			continue
		}

		// According to statistics, some epoch block even has more than 2200 event logs (eg. epoch #13329688).
		// It would be better to insert limited event logs per time for good performance. With some testing, the
		// limited number set to 800 is ok for the momemt with slow sql statements hardly found in our log files.
		// TODO more benchmarks are needed for optimization.
		maxInsertLogs := 800
		rounds := len(trxlogs) / maxInsertLogs
		if len(trxlogs)%maxInsertLogs != 0 {
			rounds++
		}

		for m, start := 1, 0; m <= rounds; m++ {
			end := util.MinInt(m*maxInsertLogs, len(trxlogs))
			if err := dbTx.Create(trxlogs[start:end]).Error; err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"start": start, "end": end, "blockHash": block.Hash,
				}).Error("Failed to insert transaction event logs part to database")

				return insertLogIdSpan, opHistory, errors.WithMessagef(err, "Failed to batch write event logs for block #%v", block.Hash)
			}
			start = end
		}

		// accumulate inserted logs id span
		insertLogIdSpan[0] = util.MinUint64(trxlogs[0].ID, insertLogIdSpan[0])
		insertLogIdSpan[1] = util.MaxUint64(trxlogs[len(trxlogs)-1].ID, insertLogIdSpan[1])

		opHistory[store.EpochLog] += int64(len(trxlogs))
	}

	return insertLogIdSpan, opHistory, nil
}

func (ms *mysqlStore) remove(epochFrom, epochTo uint64, option store.EpochRemoveOption, newOpAffects func() *mysqlEpochDataOpAffects) error {
	updater := metrics.NewTimerUpdaterByName("infura/store/mysql/delete")
	defer updater.Update()

	txOpAffects := newOpAffects()
	err := ms.execWithTx(func(dbTx *gorm.DB) (*mysqlEpochDataOpAffects, error) {
		deleteBeforeLogsPartEpochRanges, err := map[string]citypes.EpochRange{}, error(nil)
		if option&store.EpochRemoveLog != 0 {
			// get relative epoch ranges for logs table partitions before deletion for diff late
			deleteBeforeLogsPartEpochRanges, err = ms.loadLikelyActionLogsPartEpochRangesTx(dbTx, txOpAffects.OpType)
			if err != nil {
				return txOpAffects, errors.WithMessage(err, "failed to load logs partitions epoch ranges before deletion")
			}
		}

		// Batch delete for better performance
		cond := fmt.Sprintf("epoch BETWEEN %v AND %v", epochFrom, epochTo)

		// Remove blocks
		if option&store.EpochRemoveBlock != 0 {
			db := dbTx.Delete(block{}, cond)
			if db.Error != nil {
				return txOpAffects, db.Error
			}

			txOpAffects.NumAlters[store.EpochBlock] -= db.RowsAffected
		}

		// Remove txs
		if option&store.EpochRemoveTransaction != 0 {
			db := dbTx.Delete(transaction{}, cond)
			if db.Error != nil {
				return txOpAffects, db.Error
			}

			txOpAffects.NumAlters[store.EpochTransaction] -= db.RowsAffected
		}

		// Remove logs
		if option&store.EpochRemoveLog != 0 {
			partitions, err := ms.findLogsPartitionsEpochRangeWithinStoreTx(dbTx, epochFrom, epochTo)
			if err != nil {
				return txOpAffects, errors.WithMessage(err, "failed to find logs partitions for deletion")
			}

			// TODO: refactor this due to there is no need to delete logs any more if no partitions found for it.
			if len(partitions) > 0 {
				dbTx = dbTx.Table(fmt.Sprintf("logs PARTITION (%v)", strings.Join(partitions, ",")))
			}

			db := dbTx.Delete(log{}, cond)
			if db.Error != nil {
				return txOpAffects, db.Error
			}

			txOpAffects.NumAlters[store.EpochLog] -= db.RowsAffected

			for _, part := range partitions {
				afterER, err := ms.loadLogsTblPartitionEpochRanges(dbTx, part)
				if err != nil && !ms.IsRecordNotFound(err) {
					return txOpAffects, errors.WithMessage(err, "failed to load logs partitions epoch ranges after deletion")
				}

				beforeER, ok := deleteBeforeLogsPartEpochRanges[part]
				if !ok {
					logrus.WithField("partition", part).Error("Unable to match epoch range for logs parition before deletion")
					return txOpAffects, errors.Errorf("unable to match epoch ranges for logs partition %v before deletion", part)
				}

				txOpAffects.logsPartEpochRangeRealSets[part] = ms.diffLogsPartitionEpochRangeForRealSet(beforeER, afterER)
			}
		}

		return txOpAffects, nil
	})

	return err
}

func (ms *mysqlStore) dequeueEpochRangeData(dt store.EpochDataType, epochUntil uint64) error {
	// Genesis block will never be dequeued
	epochUntil = util.MaxUint64(epochUntil, 1)

	// Get local epoch range for block/tx/log
	epochFrom := atomic.LoadUint64(&ms.epochRanges[dt].EpochFrom)
	if epochUntil < epochFrom {
		return nil
	}

	opAffects := store.NewEpochDataOpAffects(dt.ToDequeOption(), epochUntil)
	txOpAffects := newMysqlEpochDataOpAffects(opAffects)

	err := ms.remove(epochFrom, epochUntil, dt.ToRemoveOption(), func() *mysqlEpochDataOpAffects {
		return txOpAffects
	})

	return err
}

func (ms *mysqlStore) getEpochRange(dt store.EpochDataType) (uint64, uint64, error) {
	validate := func(minEpoch uint64, maxEpoch uint64) (uint64, uint64, error) {
		if minEpoch == math.MaxUint64 || maxEpoch == math.MaxUint64 {
			return 0, 0, gorm.ErrRecordNotFound
		}
		return minEpoch, maxEpoch, nil
	}

	if dt == store.EpochDataNil { // return global epoch range
		return validate(atomic.LoadUint64(&ms.minEpoch), atomic.LoadUint64(&ms.maxEpoch))
	}

	if er, ok := ms.epochRanges[dt]; ok { // get local epoch range for block/tx/log
		return validate(atomic.LoadUint64(&er.EpochFrom), atomic.LoadUint64(&er.EpochTo))
	}

	return validate(citypes.EpochNumberNil, citypes.EpochNumberNil)
}

func (ms *mysqlStore) updateEpochStats(opAffects *mysqlEpochDataOpAffects) {
	switch opAffects.OpType {
	case store.EpochOpPush: //for push
		ms.updateMaxEpoch(opAffects.PushUpToEpoch, opAffects.PushUpFromEpoch)
	case store.EpochOpPop: // for pop
		ms.updateMaxEpoch(opAffects.PopUntilEpoch - 1)
	case store.EpochOpDequeueBlock: // for dequeue...
		ms.updateMinEpoch(store.EpochBlock, opAffects.DequeueUntilEpoch+1)
	case store.EpochOpDequeueTx:
		ms.updateMinEpoch(store.EpochTransaction, opAffects.DequeueUntilEpoch+1)
	case store.EpochOpDequeueLog:
		ms.updateMinEpoch(store.EpochLog, opAffects.DequeueUntilEpoch+1)
	}

	// Update epoch totals
	ms.updateEpochTotals(opAffects.NumAlters)

	// update logs table partitions
	ms.updateLogsTablePartitions(opAffects)
}

func (ms *mysqlStore) updateMinEpoch(dt store.EpochDataType, newMinEpoch uint64) {
	// Update min epoch for local epoch range
	atomic.StoreUint64(&ms.epochRanges[dt].EpochFrom, newMinEpoch)

	// Update global epoch ranges
	minEpoch := atomic.LoadUint64(&ms.minEpoch)
	for _, t := range store.OpEpochDataTypes {
		minEpoch = util.MinUint64(minEpoch, atomic.LoadUint64(&ms.epochRanges[t].EpochFrom))
	}
	atomic.StoreUint64(&ms.minEpoch, minEpoch)
}

func (ms *mysqlStore) updateMaxEpoch(newMaxEpoch uint64, growFrom ...uint64) {
	// Update global epoch range
	atomic.StoreUint64(&ms.maxEpoch, newMaxEpoch)

	// Update local epoch ranges
	for _, t := range store.OpEpochDataTypes {
		if !ms.disabler.IsDisabledForType(t) {
			atomic.StoreUint64(&ms.epochRanges[t].EpochTo, newMaxEpoch)
		}
	}

	// Update global min epoch range if necessary (only when initial loading)
	if len(growFrom) == 0 || !atomic.CompareAndSwapUint64(&ms.minEpoch, math.MaxUint64, growFrom[0]) {
		return
	}

	// Update all local min epoch ranges
	for _, t := range store.OpEpochDataTypes {
		if !ms.disabler.IsDisabledForType(t) {
			atomic.CompareAndSwapUint64(&ms.epochRanges[t].EpochFrom, math.MaxUint64, growFrom[0])
		}
	}
}

func (ms *mysqlStore) updateEpochTotals(opHistory store.EpochDataOpNumAlters) {
	safeDecrement := func(k store.EpochDataType, v int64) {
		for { // optimistic spin lock for concurrency safe
			oldTotal, newTotal := atomic.LoadUint64(ms.epochTotals[k]), uint64(0)
			absV := -v

			if oldTotal < uint64(absV) {
				logrus.Warn("DB store epoch totals decremented underflow")
			} else {
				newTotal = oldTotal - uint64(absV)
			}

			if atomic.CompareAndSwapUint64(ms.epochTotals[k], oldTotal, newTotal) {
				break
			}
		}
	}

	// Update epoch totals
	for k, v := range opHistory {
		switch {
		case v == 0:
			continue
		case v > 0: // increase
			atomic.AddUint64(ms.epochTotals[k], uint64(v))
		case v < 0: // decrease
			safeDecrement(k, v)
		}
	}
}

func (ms *mysqlStore) updateLogsTablePartitions(opAffects *mysqlEpochDataOpAffects) {
	if len(opAffects.logsPartIndexSets) == 0 {
		return
	}

	switch opAffects.OpType {
	case store.EpochOpPush:
		maxUsed := atomic.LoadUint64(&ms.maxUsedLogsTblPartIdx)
		for _, partIdx := range opAffects.logsPartIndexSets {
			maxUsed = util.MaxUint64(maxUsed, partIdx)
		}
		atomic.StoreUint64(&ms.maxUsedLogsTblPartIdx, maxUsed)
	case store.EpochOpPop:
		maxUsed := atomic.LoadUint64(&ms.maxUsedLogsTblPartIdx)
		for _, partIdx := range opAffects.logsPartIndexSets {
			maxUsed = util.MinUint64(maxUsed, partIdx)
		}
		atomic.StoreUint64(&ms.maxUsedLogsTblPartIdx, maxUsed)
	case store.EpochOpDequeueLog:
		minUsed := atomic.LoadUint64(&ms.minUsedLogsTblPartIdx)
		for _, partIdx := range opAffects.logsPartIndexSets {
			minUsed = util.MaxUint64(minUsed, partIdx)
		}
		atomic.StoreUint64(&ms.minUsedLogsTblPartIdx, minUsed)
	default:
		return
	}
}

func (ms *mysqlStore) updateEpochStatsWithTx(dbTx *gorm.DB, opAffects *mysqlEpochDataOpAffects) (err error) {
	switch opAffects.OpType {
	case store.EpochOpPush: //for push
		err = ms.updateMaxEpochTx(dbTx, opAffects.PushUpToEpoch, opAffects.PushUpFromEpoch)
	case store.EpochOpPop: // for pop
		err = ms.updateMaxEpochTx(dbTx, opAffects.PopUntilEpoch-1)
	case store.EpochOpDequeueBlock: // for dequeue...
		err = ms.updateMinEpochTx(dbTx, store.EpochBlock, opAffects.DequeueUntilEpoch+1)
	case store.EpochOpDequeueTx:
		err = ms.updateMinEpochTx(dbTx, store.EpochTransaction, opAffects.DequeueUntilEpoch+1)
	case store.EpochOpDequeueLog:
		err = ms.updateMinEpochTx(dbTx, store.EpochLog, opAffects.DequeueUntilEpoch+1)
	}

	if err != nil {
		logrus.WithError(err).Error("Failed to update epoch range statistics")
		return err
	}

	if err = ms.updateEpochTotalsTx(dbTx, opAffects.NumAlters); err != nil {
		logrus.WithError(err).Error("Failed to update epoch total statistics")
		return err
	}

	if err = ms.updateLogsTablePartitionsTx(dbTx, opAffects); err != nil {
		logrus.WithError(err).Error("Failed to update epoch range of logs table partitions statistics")
		return err
	}

	return nil
}

func (ms *mysqlStore) updateMaxEpochTx(dbTx *gorm.DB, newMaxEpoch uint64, growFrom ...uint64) error {
	logrus.WithFields(logrus.Fields{
		"newMaxEpoch": newMaxEpoch, "growFrom": growFrom,
	}).Debug("Update max of epoch range in db store")

	keys := []string{getEpochRangeStatsKey(store.EpochDataNil)}
	for _, t := range store.OpEpochDataTypes {
		if !ms.disabler.IsDisabledForType(t) {
			keys = append(keys, getEpochRangeStatsKey(t))
		}
	}

	cond := map[string]interface{}{
		"type": epochStatsEpochRange, "key": keys,
	}
	updates := map[string]interface{}{"epoch2": newMaxEpoch}

	if err := dbTx.Model(epochStats{}).Where(cond).Updates(updates).Error; err != nil {
		return err
	}

	// Update min epoch range if necessary (only when initial loading)
	if len(growFrom) == 0 || atomic.LoadUint64(&ms.minEpoch) != math.MaxUint64 {
		return nil
	}

	cond["epoch1"] = citypes.EpochNumberNil
	updates = map[string]interface{}{"epoch1": growFrom[0]}

	return dbTx.Model(epochStats{}).Where(cond).Updates(updates).Error
}

func (ms *mysqlStore) updateMinEpochTx(dbTx *gorm.DB, dt store.EpochDataType, newMinEpoch uint64) error {
	logrus.WithField("newMinEpoch", newMinEpoch).Debug("Update min of epoch range in db store")

	keys := []string{getEpochRangeStatsKey(dt)}
	if atomic.LoadUint64(&ms.minEpoch) > newMinEpoch {
		keys = append(keys, getEpochRangeStatsKey(store.EpochDataNil))
	}

	cond := map[string]interface{}{
		"type": epochStatsEpochRange,
		"key":  keys,
	}
	updates := map[string]interface{}{
		"epoch1": newMinEpoch,
	}
	return dbTx.Model(epochStats{}).Where(cond).Updates(updates).Error
}

func (ms *mysqlStore) updateEpochTotalsTx(dbTx *gorm.DB, opHistory store.EpochDataOpNumAlters) (err error) {
	for t, cnt := range opHistory {
		cond := map[string]interface{}{
			"type": epochStatsEpochTotal, "key": getEpochTotalStatsKey(t),
		}

		switch {
		case cnt == 0:
			continue
		case cnt > 0: // increase
			err = dbTx.Model(epochStats{}).Where(cond).UpdateColumn("epoch1", gorm.Expr("epoch1 + ?", cnt)).Error
		case cnt < 0: // decrease
			err = dbTx.Model(epochStats{}).Where(cond).UpdateColumn("epoch1", gorm.Expr("GREATEST(0, CAST(epoch1 AS SIGNED) - ?)", -cnt)).Error
		}

		if err != nil {
			break
		}
	}

	return
}

func (ms *mysqlStore) updateLogsTablePartitionsTx(dbTx *gorm.DB, opAffects *mysqlEpochDataOpAffects) (err error) {
	if len(opAffects.logsPartEpochRangeRealSets) == 0 {
		return nil
	}

	for partName, erSet := range opAffects.logsPartEpochRangeRealSets {
		if erSet[0] == nil && erSet[1] == nil {
			continue
		}

		idx, err := ms.getLogsPartitionIndexByName(partName)
		if err != nil {
			logrus.WithField("partition", partName).WithError(err).Error("Failed to parse logs partition index from name")
			return err
		}
		opAffects.logsPartIndexSets = append(opAffects.logsPartIndexSets, idx)

		cond := map[string]interface{}{"type": epochStatsLogsPartEpochRange, "key": partName}
		updates := map[string]interface{}{}

		if erSet[0] != nil {
			updates["epoch1"] = *(erSet[0])
		}

		if erSet[1] != nil {
			updates["epoch2"] = *(erSet[1])
		}

		if err := dbTx.Model(epochStats{}).Where(cond).Updates(updates).Error; err != nil {
			logrus.WithFields(logrus.Fields{
				"cond": cond, "updates": updates,
			}).WithError(err).Error("Failed to update new epoch range for logs paritition")

			return err
		}
	}

	return nil
}

// Load epoch ranges of possibly active logs partitions to be operated on.
func (ms *mysqlStore) loadLikelyActionLogsPartEpochRangesTx(dbTx *gorm.DB, opType store.EpochOpType) (map[string]citypes.EpochRange, error) {
	minUsedPart := atomic.LoadUint64(&ms.minUsedLogsTblPartIdx)
	maxUsedPart := atomic.LoadUint64(&ms.maxUsedLogsTblPartIdx)

	partIdxs := make([]uint64, 0, 2)
	partLogsEpochRanges := map[string]citypes.EpochRange{}

	switch opType {
	case store.EpochOpPush: // push might grow logs data to a bigger partition
		partIdxs = append(partIdxs, maxUsedPart, maxUsedPart+1)
	case store.EpochOpPop: // pop might shrink logs data to a smaller parition
		if maxUsedPart > 0 {
			partIdxs = append(partIdxs, maxUsedPart-1)
		}
		partIdxs = append(partIdxs, maxUsedPart)
	case store.EpochOpDequeueLog: // dequeue might shrink logs data from a bigger partition
		partIdxs = append(partIdxs, minUsedPart, minUsedPart+1)
	default:
		return partLogsEpochRanges, errors.Errorf("invalid epoch op type %v", opType)
	}

	for _, pidx := range partIdxs {
		if pidx > LogsTablePartitionsNum { // overflow
			logrus.WithField("partitionIndex", pidx).Warn("Logs table partitions index out of bound")
			break
		}

		partName := ms.getLogsPartitionNameByIdx(pidx)
		er, err := ms.loadLogsTblPartitionEpochRanges(dbTx, partName)
		if err != nil && !ms.IsRecordNotFound(err) {
			return partLogsEpochRanges, err
		}

		partLogsEpochRanges[partName] = er
	}

	return partLogsEpochRanges, nil
}
