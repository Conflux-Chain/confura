package mysql

import (
	"fmt"
	"hash/fnv"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// Address indexed logs are used to filter event logs by contract address and optional block number.
// Generally, most contracts have limited event logs and need not to specify the epoch/block range filter.
// For some active contracts, e.g. USDT, that have many event logs, could store in separate tables.

type AddressIndexedLog struct {
	ID          uint64
	ContractID  uint64 `gorm:"column:cid;size:64;not null;index:idx_cid_bn,priority:1"`
	BlockNumber uint64 `gorm:"column:bn;not null;index:idx_cid_bn,priority:2"`
	Epoch       uint64 `gorm:"not null;index"` // to support pop logs when reorg
	Topic0      string `gorm:"size:66;not null"`
	Topic1      string `gorm:"size:66"`
	Topic2      string `gorm:"size:66"`
	Topic3      string `gorm:"size:66"`
	LogIndex    uint64 `gorm:"not null"`
	Extra       []byte `gorm:"type:mediumText"` // extra data in JSON format
}

func (AddressIndexedLog) TableName() string {
	return "addr_logs"
}

func NewAddressIndexedLog(log *types.Log, contractId, blockNumber uint64, ext *store.LogExtra) *AddressIndexedLog {
	return &AddressIndexedLog{
		ContractID:  contractId,
		BlockNumber: blockNumber,
		Epoch:       log.EpochNumber.ToInt().Uint64(),
		Topic0:      convertLogTopic(log, 0),
		Topic1:      convertLogTopic(log, 1),
		Topic2:      convertLogTopic(log, 2),
		Topic3:      convertLogTopic(log, 3),
		LogIndex:    log.LogIndex.ToInt().Uint64(),
		Extra:       mustMarshalLogExtraData(log, ext),
	}
}

// TODO delete me once integrated with RPC
func (l *AddressIndexedLog) ToRpcLog(cs *ContractStore) (*types.Log, *store.LogExtra, error) {
	log, ext, err := silentUnmarshalLogExtraData(l.Extra)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "Failed to unmarshal extra data for address indexed log")
	}

	contract, ok, err := cs.GetContractById(l.ContractID)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "Failed to get contract by id")
	}

	if !ok {
		return nil, nil, errors.New("Contract not found")
	}

	address, err := cfxaddress.NewFromBase32(contract.Address)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "Failed to parse contract address")
	}

	log.Address = address
	log.EpochNumber = types.NewBigInt(l.Epoch)
	log.Topics = constructLogTopics(l.Topic0, l.Topic1, l.Topic2, l.Topic3)

	return &log, ext, nil
}

func (log *AddressIndexedLog) cmp(other *AddressIndexedLog) int {
	if log.BlockNumber < other.BlockNumber {
		return -1
	}

	if log.BlockNumber > other.BlockNumber {
		return 1
	}

	if log.LogIndex < other.LogIndex {
		return -1
	}

	if log.LogIndex > other.LogIndex {
		return 1
	}

	return 0
}

type AddressIndexedLogSlice []*AddressIndexedLog

func (s AddressIndexedLogSlice) Len() int           { return len(s) }
func (s AddressIndexedLogSlice) Less(i, j int) bool { return s[i].cmp(s[j]) < 0 }
func (s AddressIndexedLogSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// AddressIndexedLogStore is used to store address indexed event logs in N partitions, e.g. logs_1, logs_2, ..., logs_n.
type AddressIndexedLogStore struct {
	partitionedStore
	db         *gorm.DB
	cs         *ContractStore
	model      AddressIndexedLog
	partitions uint32
}

func NewAddressIndexedLogStore(db *gorm.DB, cs *ContractStore, partitions uint32) *AddressIndexedLogStore {
	return &AddressIndexedLogStore{
		db:         db,
		cs:         cs,
		partitions: partitions,
	}
}

// CreatePartitionedTables initializes partitioned tables.
func (ls *AddressIndexedLogStore) CreatePartitionedTables() (int, error) {
	return ls.createPartitionedTables(ls.db, &ls.model, 0, ls.partitions)
}

// getPartitionByAddress returns the partition by specified contract address.
func (ls *AddressIndexedLogStore) getPartitionByAddress(contract string) uint32 {
	hasher := fnv.New32()
	hasher.Write([]byte(contract))
	// could use consistent hashing if repartition supported
	return hasher.Sum32() % ls.partitions
}

// convertToPartitionedLogs converts the specified epoch data into partitioned event logs.
func (ls *AddressIndexedLogStore) convertToPartitionedLogs(data *store.EpochData) (map[uint32][]*AddressIndexedLog, error) {
	partition2Logs := make(map[uint32][]*AddressIndexedLog)

	// divide event logs into different partitions by address
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
				// TODO ignore event logs of big contracts, which will be stored in separate tables.

				contract, _, err := ls.cs.AddContractIfAbsent(v.Address.MustGetBase32Address())
				if err != nil {
					return nil, err
				}

				var logext *store.LogExtra
				if receiptExt != nil {
					logext = receiptExt.LogExts[i]
				}

				log := NewAddressIndexedLog(&v, contract.ID, bn, logext)
				partition := ls.getPartitionByAddress(v.Address.MustGetBase32Address())
				partition2Logs[partition] = append(partition2Logs[partition], log)
			}
		}
	}

	return partition2Logs, nil
}

// AddAddressIndexedLogs adds event logs of specified epoch into different partitioned tables.
func (ls *AddressIndexedLogStore) AddAddressIndexedLogs(dbTx *gorm.DB, data *store.EpochData) error {
	// divide event logs into different partitions by address
	partition2Logs, err := ls.convertToPartitionedLogs(data)
	if err != nil {
		return err
	}

	// Insert address indexed logs into different partitions.
	for partition, logs := range partition2Logs {
		tableName := ls.getPartitionedTableName(&ls.model, partition)
		if err := dbTx.Table(tableName).CreateInBatches(&logs, defaultBatchSizeLogInsert).Error; err != nil {
			return err
		}
	}

	return nil
}

// DeleteAddressIndexedLogs removes event logs of specified epoch number range.
//
// Generally, this is used when pivot chain switched for confirmed blocks.
func (ls *AddressIndexedLogStore) DeleteAddressIndexedLogs(dbTx *gorm.DB, epochFrom, epochTo uint64) error {
	// Delete logs for all partitions in batch
	for i := uint32(0); i < ls.partitions; i++ {
		tableName := ls.getPartitionedTableName(&ls.model, i)
		sql := fmt.Sprintf("DELETE FROM %v WHERE epoch BETWEEN ? AND ?", tableName)
		if err := dbTx.Exec(sql, epochFrom, epochTo).Error; err != nil {
			return err
		}
	}

	return nil
}

// GetAddressIndexedLogs returns event logs for the specified filter.
func (ls *AddressIndexedLogStore) GetAddressIndexedLogs(
	filter AddressIndexedLogFilter,
	contract string,
) ([]*AddressIndexedLog, error) {
	partition := ls.getPartitionByAddress(contract)
	filter.TableName = ls.getPartitionedTableName(&ls.model, partition)
	return filter.Find(ls.db)
}
