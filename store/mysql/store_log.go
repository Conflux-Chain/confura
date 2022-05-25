package mysql

import (
	"fmt"
	"math"
	"strings"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/conflux-chain/conflux-infura/util/metrics"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type log struct {
	ID              uint64
	Epoch           uint64 `gorm:"not null;index"`
	BlockNumber     uint64 `gorm:"not null;index"`
	BlockHashId     uint64 `gorm:"not null;index"`
	BlockHash       string `gorm:"size:66;not null"`
	ContractAddress string `gorm:"size:64;not null"`
	Topic0          string `gorm:"size:66;not null"`
	Topic1          string `gorm:"size:66"`
	Topic2          string `gorm:"size:66"`
	Topic3          string `gorm:"size:66"`
	Data            []byte `gorm:"type:MEDIUMBLOB"`
	DataLen         uint64 `gorm:"not null"`
	TxHash          string `gorm:"size:66;not null"`
	TxIndex         uint64 `gorm:"not null"`
	TxLogIndex      uint64 `gorm:"not null"`
	LogIndex        uint64 `gorm:"not null"`
	Extra           []byte `gorm:"type:text"` // dynamic fields within json string for extention
}

func newLog(blockNumber uint64, data *types.Log, extra *store.LogExtra) *log {
	log := &log{
		Epoch:           data.EpochNumber.ToInt().Uint64(),
		BlockNumber:     blockNumber,
		BlockHash:       data.BlockHash.String(),
		ContractAddress: data.Address.MustGetBase32Address(),
		Data:            []byte(data.Data),
		Topic0:          data.Topics[0].String(),
		TxHash:          data.TransactionHash.String(),
		TxIndex:         data.TransactionIndex.ToInt().Uint64(),
		TxLogIndex:      data.TransactionLogIndex.ToInt().Uint64(),
		LogIndex:        data.LogIndex.ToInt().Uint64(),
	}

	log.BlockHashId = util.GetShortIdOfHash(log.BlockHash)
	log.DataLen = uint64(len(log.Data))

	numTopics := len(data.Topics)

	if numTopics > 1 {
		log.Topic1 = data.Topics[1].String()
	}

	if numTopics > 2 {
		log.Topic2 = data.Topics[2].String()
	}

	if numTopics > 3 {
		log.Topic3 = data.Topics[3].String()
	}

	if extra != nil {
		log.Extra = util.MustMarshalJson(extra)
	}

	return log
}

func (log *log) toRPCLog() types.Log {
	blockHash := types.Hash(log.BlockHash)
	txHash := types.Hash(log.TxHash)

	topics := []types.Hash{types.Hash(log.Topic0)}

	// in case of empty hex string with 0x prefix
	if len(log.Topic1) > 2 {
		topics = append(topics, types.Hash(log.Topic1))
	}

	if len(log.Topic2) > 2 {
		topics = append(topics, types.Hash(log.Topic2))
	}

	if len(log.Topic3) > 2 {
		topics = append(topics, types.Hash(log.Topic3))
	}

	return types.Log{
		Address:             cfxaddress.MustNewFromBase32(log.ContractAddress),
		Topics:              topics,
		Data:                types.NewBytes(log.Data),
		BlockHash:           &blockHash,
		EpochNumber:         types.NewBigInt(log.Epoch),
		TransactionHash:     &txHash,
		TransactionIndex:    types.NewBigInt(log.TxIndex),
		LogIndex:            types.NewBigInt(log.LogIndex),
		TransactionLogIndex: types.NewBigInt(log.TxLogIndex),
	}
}

func (log *log) parseLogExtra() *store.LogExtra {
	if len(log.Extra) > 0 {
		var extra store.LogExtra
		util.MustUnmarshalJson(log.Extra, &extra)

		return &extra
	}

	return nil
}

type logStore struct {
	*baseStore
	logPartitioner
}

func newLogStore(db *gorm.DB) *logStore {
	return &logStore{
		baseStore: newBaseStore(db),
	}
}

// TODO Cacheing nearhead epoch logs in memory or redis to improve performance
func (ls *logStore) GetLogs(filter store.LogFilter) ([]store.Log, error) {
	updater := metrics.Registry.Store.GetLogs()
	defer updater.Update()

	// epoch range calculated from log filter, which can be used to help locate the logs table partitions
	epochRange := citypes.RangeUint64{
		From: math.MaxUint64,
		To:   0,
	}

	switch filter.Type {
	case store.LogFilterTypeBlockHash:
		blockPart, ok, err := ls.getLogsRelBlockInfoByBlockHash(&filter)
		if err != nil {
			logrus.WithField("filter", filter).WithError(err).Info(
				"Failed to calculate epoch range for log filter of type block hash",
			)
			return nil, err
		}

		// found in database
		if ok {
			epochRange.From = blockPart.Epoch
			epochRange.To = blockPart.Epoch
			break
		}

		// otherwise, query from fullnode
		epoch, err := filter.FetchEpochByBlockHash()
		if err != nil {
			logrus.WithField("filter", filter).WithError(err).Info(
				"Failed to fetch epoch range from fullnode for log filter of type block hashes",
			)
			return nil, err
		}

		epochRange.From = epoch
		epochRange.To = epoch
	case store.LogFilterTypeBlockRange:
		blockParts, err := ls.getLogsRelBlockInfoByBlockRange(&filter)
		if err != nil {
			logrus.WithField("filter", filter).WithError(err).Info(
				"Failed to calculate epoch range for log filter of type block range",
			)
			return nil, err
		}

		for i := 0; i < len(blockParts); i++ {
			// update epoch range from store
			epochRange.From = util.MinUint64(epochRange.From, blockParts[i].Epoch)
			epochRange.To = util.MaxUint64(epochRange.To, blockParts[i].Epoch)
		}

		if len(blockParts) == len(filter.BlockRange.ToSlice()) { // already have all block numbers in store
			break
		}

		fer, err := filter.FetchEpochRangeByBlockNumber() // fetch epoch range from fullnode
		if err != nil {
			logrus.WithField("filter", filter).WithError(err).Info(
				"Failed to fetch epoch range from fullnode for log filter of type block range",
			)
			return nil, err
		}

		// update epoch range from fullnode
		epochRange.From = util.MinUint64(epochRange.From, fer.From)
		epochRange.To = util.MaxUint64(epochRange.To, fer.To)

	default:
		if filter.EpochRange != nil { // default use epoch range of log filter
			epochRange = *filter.EpochRange
		}
	}

	if epochRange.From > epochRange.To {
		logrus.WithFields(logrus.Fields{
			"filter": filter, "epochRange": epochRange,
		}).Info("Failed to calculate a valid epoch range for logs partitions (with from > to)")
		return nil, errors.New("invalid converted epoch range")
	}

	// Check if epoch ranges of the log filter are within the store.
	// TODO add a cache layer for better performance if necessary
	if _, err := ls.checkLogsEpochRangeWithinStore(epochRange.From, epochRange.To); err != nil {
		logger := logrus.WithFields(logrus.Fields{"filter": filter, "epochRange": epochRange})
		logger.WithError(err).Info("Failed to check filter epoch range within store")

		return nil, err
	}

	// calcuate logs table partitions to get logs within the filter epoch range
	logsTblPartitions, err := ls.findLogsPartitionsEpochRangeWithinStoreTx(ls.db, epochRange.From, epochRange.To)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"filter": filter, "epochRange": epochRange,
		}).WithError(err).Info("Failed to get logs partitions for filter epoch range")
		return nil, err
	}

	if len(logsTblPartitions) == 0 { // must be empty if no logs table partition(s) found
		return []store.Log{}, nil
	}

	return ls.loadLogs(filter, logsTblPartitions)
}

func (ls *logStore) loadLogs(filter store.LogFilter, partitions []string) ([]store.Log, error) {
	// Unfortunately MySQL (v5.7 as we know) will select primary as default index,
	// which will incur performance problem as table rows grow up.
	// Here we order by specified field descendingly to force use some MySQL index to
	// improve query performance.
	db := ls.db
	var prefind func()
	switch filter.Type {
	case store.LogFilterTypeBlockHash:
		db = db.Where("block_hash_id = ? AND block_hash = ?", filter.BlockHashId, filter.BlockHash)
		prefind = func() {
			db = db.Order("block_hash_id DESC")
		}
	case store.LogFilterTypeBlockRange:
		db = db.Where("block_number BETWEEN ? AND ?", filter.BlockRange.From, filter.BlockRange.To)
		db = db.Order("block_number DESC")
	case store.LogFilterTypeEpochRange:
		db = db.Where("epoch BETWEEN ? AND ?", filter.EpochRange.From, filter.EpochRange.To)
		db = db.Order("epoch DESC")
	}

	db = applyVariadicFilter(db, logColumnTypeContract, filter.Contracts)
	db = applyTopicsFilter(db, filter.Topics)

	if len(partitions) > 0 {
		db = db.Table(fmt.Sprintf("logs PARTITION (%v)", strings.Join(partitions, ",")))
	}

	if err := ls.validateCount(db, &filter); err != nil {
		return nil, err
	}

	// IMPORTANT: full node returns the last N logs.
	// To limit the number of records fetched for better performance,  we'd better retrieve
	// the logs in reverse order first, and then reverse them for the final order.
	db = db.Order("id DESC").Offset(int(filter.OffSet)).Limit(int(filter.Limit))
	if prefind != nil {
		prefind()
	}

	var logs []log
	if err := db.Find(&logs).Error; err != nil {
		logrus.WithFields(logrus.Fields{
			"filter": filter, "partitions": partitions,
		}).Error("Failed to get logs from database")
		return nil, err
	}

	result := make([]store.Log, 0, len(logs))
	for i := len(logs) - 1; i >= 0; i-- { // reverse the order for the final
		rpclog := logs[i].toRPCLog()

		var ptrExtra *store.LogExtra
		if len(logs[i].Extra) > 0 {
			var extra store.LogExtra
			util.MustUnmarshalJson(logs[i].Extra, &extra)
			ptrExtra = &extra
		}

		result = append(result, store.Log{
			CfxLog: &rpclog, Extra: ptrExtra,
		})
	}

	return result, nil
}

func (ls *logStore) validateCount(db *gorm.DB, filter *store.LogFilter) error {
	db = db.Session(&gorm.Session{}).Offset(int(store.MaxLogLimit + filter.OffSet)).Limit(1)

	var hasMore int64
	if err := db.Select("1").Find(&hasMore).Error; err != nil {
		logrus.WithField("filter", filter).WithError(err).Error(
			"Failed to validate size of result set for event logs",
		)
		return err
	}

	if hasMore > 0 {
		return store.ErrGetLogsResultSetTooLarge
	}

	return nil
}

// logsRelBlockPart logs relative data part of block model, it can be used to calcuate the epoch range for log filters
// of type block number range or block hashes.
type logsRelBlockPart struct {
	Epoch       uint64
	Hash        string // for block hash
	BlockNumber string // for block number
}

func (ls *logStore) getLogsRelBlockInfoByBlockHash(filter *store.LogFilter) (logsRelBlockPart, bool, error) {
	var result logsRelBlockPart

	db := ls.db.Model(&block{}).
		Where("hash_id = ? AND hash = ?", filter.BlockHashId, filter.BlockHash).
		Select("hash", "epoch")

	err := db.First(&result).Error
	if ls.IsRecordNotFound(err) {
		return logsRelBlockPart{}, false, nil
	}

	if err != nil {
		return logsRelBlockPart{}, false, err
	}

	return result, true, nil
}

func (ls *logStore) getLogsRelBlockInfoByBlockRange(filter *store.LogFilter) ([]logsRelBlockPart, error) {
	var res []logsRelBlockPart

	db := ls.db.Model(&block{}).
		Where("block_number IN (?)", filter.BlockRange.ToSlice()).
		Select("block_number", "epoch")

	if err := db.Find(&res).Error; err != nil {
		err = errors.WithMessage(err, "failed to get logs relative block info by number")
		return res, err
	}

	return res, nil
}

// Checks if specified epoch range of logs is within db store
func (ls *logStore) checkLogsEpochRangeWithinStore(epochFrom, epochTo uint64) (bool, error) {
	var stats epochStats
	cond := map[string]interface{}{
		"type": epochStatsEpochRange, "key": getEpochRangeStatsKey(store.EpochLog),
	}

	mdb := ls.db.Where(cond).Select("id", "epoch1", "epoch2")
	if err := mdb.First(&stats).Error; err != nil {
		return false, err
	}

	// Check if epoch logs already pruned or not.
	// Add store.Epoch1 with 1 in case of math.MaxUint64
	if (stats.Epoch1 + 1) > (epochFrom + minNumEpochsLeftToBePruned) {
		return false, store.ErrAlreadyPruned
	}

	if stats.Epoch1 <= epochFrom && stats.Epoch2 >= epochTo {
		return true, nil
	}

	return false, store.ErrNotFound
}
