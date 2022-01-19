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
	"gorm.io/gorm/clause"
	"gorm.io/hints"
)

var (
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
	db *gorm.DB

	minEpoch uint64 // minimum epoch number in database (historical data may be pruned)
	maxEpoch uint64 // maximum epoch number in database

	// Epoch range for block/transaction/log table in db
	epochRanges map[store.EpochDataType]*citypes.EpochRange
	// Total rows for block/transaction/log table in db
	epochTotals map[store.EpochDataType]*uint64

	maxUsedLogsTblPartIdx uint64 // the maximum used partition index for logs table
	minUsedLogsTblPartIdx uint64 // the minimum used partition index for logs table
}

func mustNewStore(db *gorm.DB, option StoreOption) (ms *mysqlStore) {
	ms = &mysqlStore{
		db:          db,
		minEpoch:    citypes.EpochNumberNil,
		maxEpoch:    citypes.EpochNumberNil,
		epochRanges: make(map[store.EpochDataType]*citypes.EpochRange),
		epochTotals: make(map[store.EpochDataType]*uint64),
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

	return
}

func (ms *mysqlStore) IsRecordNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, store.ErrNotFound)
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

// TODO Cacheing nearhead epoch logs in memory or redis to improve performance
func (ms *mysqlStore) GetLogs(filter store.LogFilter) ([]types.Log, error) {
	updater := metrics.NewTimerUpdaterByName("infura/store/mysql/getlogs")
	defer updater.Update()

	// epoch range calculated from log filter, which can be used to help locate the logs table partitions
	epochRange := citypes.EpochRange{EpochFrom: math.MaxUint64, EpochTo: 0}

	switch filter.Type {
	case store.LogFilterTypeBlockHashes:
		blockParts, err := ms.getLogsRelBlockInfoWithinStore(&filter)
		if err != nil {
			logrus.WithField("filter", filter).WithError(err).Error("Failed to calculate epoch range for log filter of type block hashes")
			return nil, err
		}

		foundHashes := make([]string, 0, len(blockParts))
		for i := 0; i < len(blockParts); i++ {
			foundHashes = append(foundHashes, blockParts[i].Hash)

			// update epoch range from store
			epochRange.EpochFrom = util.MinUint64(epochRange.EpochFrom, blockParts[i].Epoch)
			epochRange.EpochTo = util.MaxUint64(epochRange.EpochTo, blockParts[i].Epoch)
		}

		if len(foundHashes) == filter.BlockHashes.Count() { // already have all block hashes in store
			break
		}

		fer, err := filter.FetchEpochRangeByBlockHashes(foundHashes...) // fetch epoch range from fullnode
		if err != nil {
			return nil, errors.WithMessage(err, "failed to fetch epoch range from fullnode for log filter of type block hashes")
		}

		// update epoch range from fullnode
		epochRange.EpochFrom = util.MinUint64(epochRange.EpochFrom, fer.EpochFrom)
		epochRange.EpochTo = util.MaxUint64(epochRange.EpochTo, fer.EpochTo)

	case store.LogFilterTypeBlockRange:
		blockParts, err := ms.getLogsRelBlockInfoWithinStore(&filter)
		if err != nil {
			logrus.WithField("filter", filter).WithError(err).Error("Failed to calculate epoch range for log filter of type block range")
			return nil, err
		}

		for i := 0; i < len(blockParts); i++ {
			// update epoch range from store
			epochRange.EpochFrom = util.MinUint64(epochRange.EpochFrom, blockParts[i].Epoch)
			epochRange.EpochTo = util.MaxUint64(epochRange.EpochTo, blockParts[i].Epoch)
		}

		if len(blockParts) == len(filter.BlockRange.ToSlice()) { // already have all block numbers in store
			break
		}

		fer, err := filter.FetchEpochRangeByBlockNumber() // fetch epoch range from fullnode
		if err != nil {
			return nil, errors.WithMessage(err, "failed to fetch epoch range from fullnode for log filter of type block range")
		}

		// update epoch range from fullnode
		epochRange.EpochFrom = util.MinUint64(epochRange.EpochFrom, fer.EpochFrom)
		epochRange.EpochTo = util.MaxUint64(epochRange.EpochTo, fer.EpochTo)

	default:
		if filter.EpochRange != nil { // default use epoch range of log filter
			epochRange = *filter.EpochRange
		}
	}

	if err := citypes.ValidateEpochRange(&epochRange); err != nil {
		return nil, errors.WithMessage(err, "failed to calculate a valid epoch range for logs partitions")
	}

	// Check if epoch ranges of the log filter are within the store.
	// TODO add a cache layer for better performance if necessary
	if _, err := ms.checkLogsEpochRangeWithinStore(epochRange.EpochFrom, epochRange.EpochTo); err != nil {
		return nil, errors.WithMessage(err, "failed to check filter epoch range within store")
	}

	// calcuate logs table partitions to get logs within the filter epoch range
	logsTblPartitions, err := findLogsPartitionsEpochRangeWithinStoreTx(ms.db, epochRange.EpochFrom, epochRange.EpochTo)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get logs partitions for filter epoch range")
	}

	if len(logsTblPartitions) == 0 { // must be empty if no logs table partition(s) found
		return []types.Log{}, nil
	}

	return loadLogs(ms.db, filter, logsTblPartitions)
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

	// If unknown number of event logs (for back compatibility) or no event logs
	// exists inside transaction receipts, just skip retrieving && assembling
	// event logs for it.
	if tx.NumReceiptLogs <= 0 {
		return &receipt, nil
	}

	partitions, err := findLogsPartitionsEpochRangeWithinStoreTx(ms.db, tx.Epoch, tx.Epoch)
	if err != nil {
		err = errors.WithMessage(err, "failed to get partitions for receipt logs assembling")
		return nil, err
	}

	if len(partitions) == 0 { // no partitions found?
		return nil, errors.New("no partitions found for receipt logs assembling")
	}

	// TODO: add benchmark to see if adding transaction hash short ID as index in logs table
	// would bring better performance.
	blockHashId := util.GetShortIdOfHash(string(receipt.BlockHash))
	db := ms.db.Where(
		"epoch = ? AND block_hash_id = ? AND tx_hash = ?", tx.Epoch, blockHashId, tx.Hash,
	)
	db = db.Clauses(hints.UseIndex("idx_logs_block_hash_id"))
	db = db.Table(fmt.Sprintf("logs PARTITION (%v)", strings.Join(partitions, ",")))

	var logs []log

	if err := db.Find(&logs).Error; err != nil {
		err = errors.WithMessage(err, "failed to get data for receipt logs assembling")
		return nil, err
	}

	if len(logs) != tx.NumReceiptLogs { // validate number of assembled receipt logs
		err := errors.Errorf(
			"num of assembled receipt logs mismatched, expect %v got %v",
			tx.NumReceiptLogs, len(logs),
		)
		return nil, err
	}

	rpcLogs := make([]types.Log, 0, len(logs))
	for i := 0; i < len(logs); i++ {
		rpcLogs = append(rpcLogs, logs[i].toRPCLog())
	}

	receipt.Logs = rpcLogs
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
	// TODO Cannot get tx from db in advance, since only executed txs saved in db
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

func (ms *mysqlStore) GetBlockByBlockNumber(blockNumber uint64) (*types.Block, error) {
	return nil, store.ErrUnsupported
}

func (ms *mysqlStore) GetBlockSummaryByBlockNumber(blockNumber uint64) (*types.BlockSummary, error) {
	return loadBlock(ms.db, "block_number = ?", blockNumber)
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

		if insertLogs || len(data.Receipts) == 0 {
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
			idxStart := getLogsPartitionIdxFromId(insertLogsIDSpan[0])
			idxEnd := getLogsPartitionIdxFromId(insertLogsIDSpan[1])

			for idx := idxStart; idx <= idxEnd; idx++ {
				partition := getLogsPartitionNameByIdx(idx)
				afterER, err := loadLogsTblPartitionEpochRanges(dbTx, partition)
				if err != nil {
					return txOpAffects, errors.WithMessage(err, "failed to load logs partitions epoch ranges after push")
				}

				beforeER, ok := insertBeforeLogsPartEpochRanges[partition]
				if !ok {
					logrus.WithField("partition", partition).Error("Unable to match epoch range for logs parition before push")
					return txOpAffects, errors.Errorf("unable to match epoch ranges for logs partition %v before push", partition)
				}

				txOpAffects.logsPartEpochRangeRealSets[partition] = diffLogsPartitionEpochRangeForRealSet(beforeER, afterER)
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
	}).WithError(err).Info("Epoch data popped out from MySQL store")

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

func (ms *mysqlStore) Close() error {
	if mysqlDb, err := ms.db.DB(); err != nil {
		return err
	} else {
		return mysqlDb.Close()
	}
}

func (ms *mysqlStore) LoadConfig(confNames ...string) (map[string]interface{}, error) {
	var confs []conf

	if err := ms.db.Where("name IN ?", confNames).Find(&confs).Error; err != nil {
		return nil, err
	}

	res := make(map[string]interface{}, len(confs))
	for _, c := range confs {
		res[c.Name] = c.Value
	}

	return res, nil
}

func (ms *mysqlStore) StoreConfig(confName string, confVal interface{}) error {
	return ms.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"value": confVal}),
	}).Create(&conf{
		Name: confName, Value: confVal.(string),
	}).Error
}

// calibrateEpochStats calibrates epoch statistics by running MySQL OLAP.
func (ms *mysqlStore) calibrateEpochStats() error {
	var count int64
	if err := ms.db.Model(&epochStats{}).Count(&count).Error; err != nil {
		return errors.WithMessage(err, "failed to count epoch stats table")
	}

	if count > 0 { // already calibrated with records in epoch_stats table
		return ms.loadCalibratedEpochStats()
	}

	for _, t := range store.OpEpochDataTypes {
		// load epoch range
		minEpoch, maxEpoch, err := ms.loadEpochRange(t)

		if err != nil && !ms.IsRecordNotFound(err) {
			return errors.WithMessage(err, "failed to load epoch range")
		}

		er := citypes.EpochRange{EpochFrom: minEpoch, EpochTo: maxEpoch}
		if err == nil { // update global epoch range
			ms.minEpoch = util.MinUint64(ms.minEpoch, minEpoch)

			if ms.maxEpoch != math.MaxUint64 {
				ms.maxEpoch = util.MaxUint64(ms.maxEpoch, maxEpoch)
			} else { // initial setting
				ms.maxEpoch = maxEpoch
			}
		}
		ms.epochRanges[t] = &er

		// load epoch total
		total, err := ms.loadEpochTotal(t)
		if err != nil {
			return errors.WithMessage(err, "failed to load epoch total")
		}

		ms.epochTotals[t] = &total
	}

	// store epoch statistics to epoch_stats table
	dbTx := ms.db.Begin()
	if dbTx.Error != nil {
		return errors.WithMessage(dbTx.Error, "failed to begin db tx")
	}

	rollback := func(err error) error {
		if rollbackErr := dbTx.Rollback().Error; rollbackErr != nil {
			logrus.WithError(rollbackErr).Error("Failed to rollback db tx")
		}

		return errors.WithMessage(err, "failed to handle with db tx")
	}

	// store epoch ranges
	er := citypes.EpochRange{EpochFrom: ms.minEpoch, EpochTo: ms.maxEpoch}
	if err := initOrUpdateEpochRangeStats(dbTx, store.EpochDataNil, er); err != nil {
		return rollback(errors.WithMessage(err, "failed to update global epoch range stats"))
	}

	for _, dt := range store.OpEpochDataTypes {
		epr := ms.epochRanges[dt]
		if err := initOrUpdateEpochRangeStats(dbTx, dt, *epr); err != nil {
			return rollback(errors.WithMessage(err, "failed to update local epoch range stats"))
		}

		ept := ms.epochTotals[dt]
		if err := initOrUpdateEpochTotalsStats(dbTx, dt, *ept); err != nil {
			return rollback(errors.WithMessage(err, "failed to update epoch total stats"))
		}
	}

	// also calculate epoch ranges of logs table partitions and save them to epoch_stats table
	partitionNames, err := loadLogsTblPartitionNames(dbTx)
	if err != nil {
		return rollback(errors.WithMessage(err, "failed to get logs table partition names"))
	}

	var minUsedPart, maxUsedPart uint64
	minUsedPart, maxUsedPart = math.MaxUint64, 0

	for i, partName := range partitionNames {
		partEpochRange, err := loadLogsTblPartitionEpochRanges(dbTx, partName)

		if err != nil && !ms.IsRecordNotFound(err) {
			return rollback(errors.WithMessagef(err, "failed to get epoch range for logs partition %v", partName))
		} else if err == nil {
			minUsedPart = util.MinUint64(minUsedPart, uint64(i))
			maxUsedPart = util.MaxUint64(maxUsedPart, uint64(i))
		}

		if err := initOrUpdateLogsPartitionEpochRangeStats(dbTx, partName, partEpochRange); err != nil {
			return rollback(errors.WithMessagef(err, "failed to write epoch range for logs partition %v to epoch stats", partName))
		}
	}

	if minUsedPart != math.MaxUint64 {
		atomic.StoreUint64(&ms.maxUsedLogsTblPartIdx, maxUsedPart)
		atomic.StoreUint64(&ms.minUsedLogsTblPartIdx, minUsedPart)
	}

	if err := dbTx.Commit().Error; err != nil {
		return errors.WithMessage(err, "failed to commit db tx")
	}

	return nil
}

func (ms *mysqlStore) loadCalibratedEpochStats() error {
	// load epoch range statistics from epoch_stats table
	erStats, err := loadEpochStats(ms.db, epochStatsEpochRange)
	if err != nil {
		return errors.WithMessage(err, "failed to load calibrated epoch range stats")
	}

	for _, stats := range erStats {
		edt := getEpochDataTypeByEpochRangeStatsKey(stats.Key)
		if edt == store.EpochDataNil {
			atomic.StoreUint64(&ms.minEpoch, stats.Epoch1)
			atomic.StoreUint64(&ms.maxEpoch, stats.Epoch2)
			continue
		}

		ms.epochRanges[edt] = &citypes.EpochRange{
			EpochFrom: stats.Epoch1, EpochTo: stats.Epoch2,
		}
	}

	// load epoch total statistics from epoch_stats table
	etStats, err := loadEpochStats(ms.db, epochStatsEpochTotal)
	if err != nil {
		return errors.WithMessage(err, "failed to load calibrated epoch total stats")
	}

	for _, stats := range etStats {
		edt := getEpochDataTypeByEpochTotalStatsKey(stats.Key)
		totalNum := stats.Epoch1
		ms.epochTotals[edt] = &totalNum
	}

	// also calculate used logs table partition indexes
	var logsPartStats []epochStats

	mdb := ms.db.Where("`type` = ?", epochStatsLogsPartEpochRange)
	mdb = mdb.Where("`key` LIKE ?", "logs%")
	mdb = mdb.Where("`epoch1` <> ?", citypes.EpochNumberNil)
	if err := mdb.Model(&epochStats{}).Select("key").Order("id ASC").Find(&logsPartStats).Error; err != nil {
		return errors.WithMessage(err, "failed to load calibrated logs table partitions epoch range stats")
	}

	if len(logsPartStats) == 0 { // no used logs partitions at all
		return nil
	}

	minUsedPart, err := getLogsPartitionIndexByName(logsPartStats[0].Key)
	if err != nil {
		return errors.WithMessagef(err, "failed to get min used index with logs partition %v", logsPartStats[0].Key)
	}

	maxUsedPart, err := getLogsPartitionIndexByName(logsPartStats[len(logsPartStats)-1].Key)
	if err != nil {
		return errors.WithMessagef(err, "failed to get max used index with logs partition %v", logsPartStats[len(logsPartStats)-1].Key)
	}

	atomic.StoreUint64(&ms.maxUsedLogsTblPartIdx, maxUsedPart)
	atomic.StoreUint64(&ms.minUsedLogsTblPartIdx, minUsedPart)

	return nil
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
		if err := dbTx.Create(newBlock(block, i == pivotIndex)).Error; err != nil {
			return insertLogIdSpan, opHistory, errors.WithMessagef(err, "Failed to write block #%v", block.Hash)
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
				blockNum := block.BlockNumber.ToInt().Uint64()
				trxlogs = append(trxlogs, newLog(blockNum, &log))
			}
		}

		// Batch insert block transactions
		if len(trxs) == 0 {
			continue
		}

		opHistory[store.EpochTransaction] += int64(len(trxs))

		if err := dbTx.Create(trxs).Error; err != nil {
			return insertLogIdSpan, opHistory, errors.WithMessagef(err, "Failed to batch write txs and receipts for block #%v", block.Hash)
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

		for i, start := 1, 0; i <= rounds; i++ {
			end := util.MinInt(i*maxInsertLogs, len(trxlogs))
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
			partitions, err := findLogsPartitionsEpochRangeWithinStoreTx(dbTx, epochFrom, epochTo)
			if err != nil {
				return txOpAffects, errors.WithMessage(err, "failed to find logs partitions for deletion")
			}

			if len(partitions) > 0 {
				dbTx = dbTx.Table(fmt.Sprintf("logs PARTITION (%v)", strings.Join(partitions, ",")))
			}

			db := dbTx.Delete(log{}, cond)
			if db.Error != nil {
				return txOpAffects, db.Error
			}

			txOpAffects.NumAlters[store.EpochLog] -= db.RowsAffected

			for _, part := range partitions {
				afterER, err := loadLogsTblPartitionEpochRanges(dbTx, part)
				if err != nil && !ms.IsRecordNotFound(err) {
					return txOpAffects, errors.WithMessage(err, "failed to load logs partitions epoch ranges after deletion")
				}

				beforeER, ok := deleteBeforeLogsPartEpochRanges[part]
				if !ok {
					logrus.WithField("partition", part).Error("Unable to match epoch range for logs parition before deletion")
					return txOpAffects, errors.Errorf("unable to match epoch ranges for logs partition %v before deletion", part)
				}

				txOpAffects.logsPartEpochRangeRealSets[part] = diffLogsPartitionEpochRangeForRealSet(beforeER, afterER)
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

	opAffects := store.NewEpochDataOpAffects(store.EpochDataTypeDequeueOptionMap[dt], epochUntil)
	txOpAffects := newMysqlEpochDataOpAffects(opAffects)

	err := ms.remove(epochFrom, epochUntil, store.EpochDataTypeRemoveOptionMap[dt], func() *mysqlEpochDataOpAffects {
		return txOpAffects
	})

	return err
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

// logsRelBlockPart logs relative data part of block model, it can be used to calcuate the epoch range for log filters
// of type block number range or block hashes.
type logsRelBlockPart struct {
	Epoch       uint64
	Hash        string // for block hash
	BlockNumber string // for block number
}

func (ms *mysqlStore) getLogsRelBlockInfoWithinStore(filter *store.LogFilter) ([]logsRelBlockPart, error) {
	var res []logsRelBlockPart
	db := ms.db.Model(&block{})

	switch filter.Type {
	case store.LogFilterTypeBlockHashes:
		db = applyVariadicFilter(db, "hash_id", filter.BlockHashIds)
		db = applyVariadicFilter(db, "hash", filter.BlockHashes)

		if err := db.Select("hash", "epoch").Find(&res).Error; err != nil {
			err = errors.WithMessage(err, "failed to get logs relative block info by hash")
			return res, err
		}
	case store.LogFilterTypeBlockRange:
		blockNums := filter.BlockRange.ToSlice()
		db.Where("block_number IN (?)", blockNums)

		if err := db.Select("block_number", "epoch").Find(&res).Error; err != nil {
			err = errors.WithMessage(err, "failed to get logs relative block info by number")
			return res, err
		}
	}

	return res, nil
}

// Checks if specified epoch range of logs is within db store
func (ms *mysqlStore) checkLogsEpochRangeWithinStore(epochFrom, epochTo uint64) (bool, error) {
	var stats epochStats
	cond := map[string]interface{}{
		"type": epochStatsEpochRange, "key": getEpochRangeStatsKey(store.EpochLog),
	}

	mdb := ms.db.Where(cond).Select("id", "epoch1", "epoch2")
	if err := mdb.First(&stats).Error; err != nil {
		return false, err
	}

	// Check if epoch logs already pruned or not.
	if stats.Epoch1 > (epochFrom + minNumEpochsLeftToBePruned) {
		return false, store.ErrAlreadyPruned
	}

	if stats.Epoch1 <= epochFrom && stats.Epoch2 >= epochTo {
		return true, nil
	}

	return false, store.ErrNotFound
}

// Find the right logs table partition(s) for the specified epoch range. It will check
// logs table paritions and return all the partitions whose epoch range are overlapped
// with the specified one.
func findLogsPartitionsEpochRangeWithinStoreTx(dbTx *gorm.DB, epochFrom, epochTo uint64) (res []string, err error) {
	mdb := dbTx.Where("`type` = ? AND `key` LIKE ?", epochStatsLogsPartEpochRange, "logs%")
	mdb = mdb.Where("`epoch1` <= ? AND `epoch2` >= ?", epochTo, epochFrom)

	err = mdb.Model(&epochStats{}).Select("key").Find(&res).Error
	return
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
		atomic.StoreUint64(&ms.epochRanges[t].EpochTo, newMaxEpoch)
	}

	// Update global min epoch range if necessary (only when initial loading)
	if len(growFrom) == 0 || !atomic.CompareAndSwapUint64(&ms.minEpoch, math.MaxUint64, growFrom[0]) {
		return
	}

	// Update all local min epoch ranges
	for _, t := range store.OpEpochDataTypes {
		atomic.CompareAndSwapUint64(&ms.epochRanges[t].EpochFrom, math.MaxUint64, growFrom[0])
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
		keys = append(keys, getEpochRangeStatsKey(t))
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

		idx, err := getLogsPartitionIndexByName(partName)
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

func (ms *mysqlStore) dumpEpochRanges() string {
	strBuilder := &strings.Builder{}
	strBuilder.Grow(len(ms.epochRanges) * 30)

	for t, er := range ms.epochRanges {
		minEpoch, maxEpoch := atomic.LoadUint64(&er.EpochFrom), atomic.LoadUint64(&er.EpochTo)
		strBuilder.WriteString(fmt.Sprintf("%v:[%v,%v]; ", EpochDataTypeTableMap[t], minEpoch, maxEpoch))
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

		partName := getLogsPartitionNameByIdx(pidx)
		er, err := loadLogsTblPartitionEpochRanges(dbTx, partName)
		if err != nil && !ms.IsRecordNotFound(err) {
			return partLogsEpochRanges, err
		}

		partLogsEpochRanges[partName] = er
	}

	return partLogsEpochRanges, nil
}

func diffLogsPartitionEpochRangeForRealSet(beforeER, afterER citypes.EpochRange) [2]*uint64 {
	diff := func(before, after uint64) *uint64 {
		if before == after { // no change
			return nil
		}
		return &after
	}

	return [2]*uint64{
		diff(beforeER.EpochFrom, afterER.EpochFrom),
		diff(beforeER.EpochTo, afterER.EpochTo),
	}
}
