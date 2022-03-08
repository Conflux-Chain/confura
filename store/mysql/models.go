package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var allModels = []interface{}{
	&transaction{},
	&block{},
	&log{},
	&epochStats{},
	&conf{},
	&User{},
}

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

func loadLogs(db *gorm.DB, filter store.LogFilter, partitions []string) ([]store.Log, error) {
	// Unfortunately MySQL (v5.7 as we know) will select primary as default index,
	// which will incur performance problem as table rows grow up.
	// Here we order by specified field descendingly to force use some MySQL index to
	// improve query performance.
	var prefind func()
	switch filter.Type {
	case store.LogFilterTypeBlockHashes:
		db = applyVariadicFilter(db, "block_hash_id", filter.BlockHashIds)
		db = applyVariadicFilter(db, "block_hash", filter.BlockHashes)
		prefind = func() {
			db = db.Order("block_hash_id DESC")
		}
	case store.LogFilterTypeBlockRange:
		db = db.Where("block_number BETWEEN ? AND ?", filter.BlockRange.EpochFrom, filter.BlockRange.EpochTo)
		db = db.Order("block_number DESC")
	case store.LogFilterTypeEpochRange:
		db = db.Where("epoch BETWEEN ? AND ?", filter.EpochRange.EpochFrom, filter.EpochRange.EpochTo)
		db = db.Order("epoch DESC")
	}

	db = applyVariadicFilter(db, "contract_address", filter.Contracts)

	numTopics := len(filter.Topics)

	if numTopics > 0 {
		db = applyVariadicFilter(db, "topic0", filter.Topics[0])
	}

	if numTopics > 1 {
		db = applyVariadicFilter(db, "topic1", filter.Topics[1])
	}

	if numTopics > 2 {
		db = applyVariadicFilter(db, "topic2", filter.Topics[2])
	}

	if numTopics > 3 {
		db = applyVariadicFilter(db, "topic3", filter.Topics[3])
	}

	if len(partitions) > 0 {
		db = db.Table(fmt.Sprintf("logs PARTITION (%v)", strings.Join(partitions, ",")))
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

func parseLogExtra(log *log) *store.LogExtra {
	if len(log.Extra) > 0 {
		var extra store.LogExtra
		util.MustUnmarshalJson(log.Extra, &extra)

		return &extra
	}

	return nil
}

func applyVariadicFilter(db *gorm.DB, column string, value store.VariadicValue) *gorm.DB {
	if single, ok := value.Single(); ok {
		return db.Where(fmt.Sprintf("%v = ?", column), single)
	}

	if multiple, ok := value.FlatMultiple(); ok {
		return db.Where(fmt.Sprintf("%v IN (?)", column), multiple)
	}

	return db
}

// loadLogsTblPartitionNames retrieves all logs table partitions names sorted by partition oridinal position.
func loadLogsTblPartitionNames(db *gorm.DB, dbname string) ([]string, error) {
	sqlStatement := `
SELECT PARTITION_NAME AS partname FROM information_schema.partitions WHERE TABLE_SCHEMA='%v'
	AND TABLE_NAME = 'logs' AND PARTITION_NAME IS NOT NULL ORDER BY PARTITION_ORDINAL_POSITION ASC;
`
	sqlStatement = fmt.Sprintf(sqlStatement, dbname)

	// Load all logs table partition names
	logsTblPartiNames := []string{}
	if err := db.Raw(sqlStatement).Scan(&logsTblPartiNames).Error; err != nil {
		return nil, err
	}

	return logsTblPartiNames, nil
}

// loadLogsTblPartitionEpochRanges loads epoch range for specific partition name.
// The epoch number ranges will be used to find the right partition(s) to boost
// the db query performance when conditioned with epoch.
func loadLogsTblPartitionEpochRanges(db *gorm.DB, partiName string) (citypes.EpochRange, error) {
	sqlStatement := fmt.Sprintf("SELECT MIN(epoch) as minEpoch, MAX(epoch) as maxEpoch FROM logs PARTITION (%v)", partiName)
	row := db.Raw(sqlStatement).Row()
	if err := row.Err(); err != nil {
		return citypes.EpochRangeNil, err
	}

	var minEpoch, maxEpoch sql.NullInt64
	if err := row.Scan(&minEpoch, &maxEpoch); err != nil {
		return citypes.EpochRangeNil, err
	}

	if !minEpoch.Valid || !maxEpoch.Valid {
		return citypes.EpochRangeNil, gorm.ErrRecordNotFound
	}

	return citypes.EpochRange{
		EpochFrom: uint64(minEpoch.Int64), EpochTo: uint64(maxEpoch.Int64),
	}, nil
}

// epoch statistics
type epochStats struct {
	ID uint32
	// key name
	Key string `gorm:"index:uidx_key_type,unique;size:66;not null"`
	// stats type
	Type epochStatsType `gorm:"index:uidx_key_type,unique;not null"`

	// min epoch for epoch range or total epoch number
	Epoch1 uint64
	// max epoch for epoch range or reversed for other use
	Epoch2 uint64

	CreatedAt time.Time
	UpdatedAt time.Time
}

// TableName overrides the table name used by epochStats to `epoch_stats`
func (epochStats) TableName() string {
	return "epoch_stats"
}

func initOrUpdateEpochRangeStats(db *gorm.DB, dt store.EpochDataType, epochRange citypes.EpochRange) error {
	estats := epochStats{
		Key:    getEpochRangeStatsKey(dt),
		Type:   epochStatsEpochRange,
		Epoch1: epochRange.EpochFrom,
		Epoch2: epochRange.EpochTo,
	}

	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}, {Name: "type"}},
		DoUpdates: clause.AssignmentColumns([]string{"epoch1", "epoch2"}),
	}).Create(&estats).Error
}

func initOrUpdateEpochTotalsStats(db *gorm.DB, dt store.EpochDataType, totals uint64) error {
	estats := epochStats{
		Key:    getEpochTotalStatsKey(dt),
		Type:   epochStatsEpochTotal,
		Epoch1: totals,
	}

	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}, {Name: "type"}},
		DoUpdates: clause.AssignmentColumns([]string{"epoch1"}),
	}).Create(&estats).Error
}

func initOrUpdateLogsPartitionEpochRangeStats(db *gorm.DB, partition string, epochRange citypes.EpochRange) error {
	estats := epochStats{
		Key:    partition,
		Type:   epochStatsLogsPartEpochRange,
		Epoch1: epochRange.EpochFrom,
		Epoch2: epochRange.EpochTo,
	}

	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}, {Name: "type"}},
		DoUpdates: clause.AssignmentColumns([]string{"epoch1", "epoch2"}),
	}).Create(&estats).Error
}

func loadEpochStats(db *gorm.DB, est epochStatsType, keys ...string) ([]epochStats, error) {
	var ess []epochStats
	cond := map[string]interface{}{"type": est}
	if len(keys) > 0 {
		cond["key"] = keys
	}

	err := db.Where(cond).Find(&ess).Error
	return ess, err
}
