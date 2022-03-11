package mysql

import (
	"hash/fnv"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Address indexed logs are used to filter event logs by contract address and optional block number.
// Generally, most contracts have limited event logs and need not to specify the epoch/block range filter.
// For some active contracts, e.g. USDT, that have many event logs, could store in separate tables.

type AddressIndexedLog struct {
	ID              uint64
	ContractAddress string `gorm:"size:64;not null;index:idx_addr_bn,priority:1"` // TODO use addressId instead
	BlockNumber     uint64 `gorm:"not null;index:idx_addr_bn,priority:2"`
	Epoch           uint64 `gorm:"not null;index"` // to support pop logs when reorg
	Topic0          string `gorm:"size:66;not null"`
	Topic1          string `gorm:"size:66"`
	Topic2          string `gorm:"size:66"`
	Topic3          string `gorm:"size:66"`
	Extra           []byte `gorm:"type:text"` // extra data in JSON format
}

func (AddressIndexedLog) TableName() string {
	return "addr_logs"
}

func NewAddressIndexedLog(log *types.Log, blockNumber uint64, ext *store.LogExtra) *AddressIndexedLog {
	return &AddressIndexedLog{
		ContractAddress: log.Address.MustGetBase32Address(),
		BlockNumber:     blockNumber,
		Epoch:           log.EpochNumber.ToInt().Uint64(),
		Topic0:          convertLogTopic(log, 0),
		Topic1:          convertLogTopic(log, 1),
		Topic2:          convertLogTopic(log, 2),
		Topic3:          convertLogTopic(log, 3),
		Extra:           mustMarshalLogExtraData(log, ext),
	}
}

func (l *AddressIndexedLog) ToRpcLog() (*types.Log, *store.LogExtra) {
	reportErr := func(err error, msg string) {
		logrus.WithError(err).WithFields(logrus.Fields{
			"addr": l.ContractAddress,
			"bn":   l.BlockNumber,
		}).Error(err)
	}

	log, ext, err := silentUnmarshalLogExtraData(l.Extra)
	if err != nil {
		reportErr(err, "Failed to unmarshal extra data for address indexed log")
	}

	address, err := cfxaddress.NewFromBase32(l.ContractAddress)
	if err != nil {
		reportErr(err, "Failed to parse contract address")
	}

	log.Address = address
	log.EpochNumber = types.NewBigInt(l.Epoch)
	log.Topics = constructLogTopics(l.Topic0, l.Topic1, l.Topic2, l.Topic3)

	return &log, ext
}

type AddressIndexedLogStore struct {
	partitionedStore
	db *gorm.DB
}

func NewAddressIndexedLogStore(db *gorm.DB) *AddressIndexedLogStore {
	return &AddressIndexedLogStore{
		db: db,
	}
}

func (ls *AddressIndexedLogStore) CreateAddressIndexedLogTable(partitionFrom, count uint32) (int, error) {
	return ls.createPartitionedTables(ls.db, &AddressIndexedLog{}, partitionFrom, count)
}

func (ls *AddressIndexedLogStore) DeleteAddressIndexedLogTable(partition uint32) (bool, error) {
	return ls.deletePartitionedTable(ls.db, &AddressIndexedLog{}, partition)
}

func (ls *AddressIndexedLogStore) AddAddressIndexedLogs(dbTx *gorm.DB, data *store.EpochData, totalPartitions uint32) error {
	partition2Logs := make(map[uint32][]*AddressIndexedLog)

	for _, block := range data.Blocks {
		bn := block.BlockNumber.ToInt().Uint64()

		for _, tx := range block.Transactions {
			// ignore txs that not executed in current block
			if !util.IsTxExecutedInBlock(&tx) {
				continue
			}

			receipt, ok := data.Receipts[tx.Hash]
			if !ok {
				// should never occur, just to ensure code robust
				logrus.WithFields(logrus.Fields{
					"epoch": data.Number,
					"tx":    tx.Hash,
				}).Error("Cannot find transaction receipt in epoch data")
				continue
			}

			receiptExt := data.ReceiptExts[tx.Hash]

			for i, v := range receipt.Logs {
				// TODO ignore logs of big contracts
				log := NewAddressIndexedLog(&v, bn, receiptExt.LogExts[i])
				partition := ls.getLogPartitionByAddress(&v.Address, totalPartitions)
				partition2Logs[partition] = append(partition2Logs[partition], log)
			}
		}
	}

	var model AddressIndexedLog

	// Insert address indexed logs into different partitions.
	for partition, logs := range partition2Logs {
		tableName := ls.getPartitionedTableName(&model, partition)
		if err := dbTx.Table(tableName).CreateInBatches(&logs, defaultBatchSizeLogInsert).Error; err != nil {
			return err
		}
	}

	return nil
}

func (ls *AddressIndexedLogStore) getLogPartitionByAddress(contract *cfxaddress.Address, totalPartitions uint32) uint32 {
	hasher := fnv.New32()
	hasher.Write(contract.MustGetCommonAddress().Bytes())
	// Use consistent hashing if repartition supported
	return hasher.Sum32() % totalPartitions
}

// TODO getLogs by address and optional block number

// TODO estimate result set size for specified log filter

// TODO supports repartition with consistent hashing and data migration.
