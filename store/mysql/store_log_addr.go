package mysql

import (
	"fmt"

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
	Extra       []byte `gorm:"type:text"` // extra data in JSON format
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
		Extra:       mustMarshalLogExtraData(log, ext),
	}
}

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

func (ls *AddressIndexedLogStore) CreateAddressIndexedLogTable(partitionFrom, count uint32) (int, error) {
	return ls.createPartitionedTables(ls.db, &AddressIndexedLog{}, partitionFrom, count)
}

func (ls *AddressIndexedLogStore) AddAddressIndexedLogs(dbTx *gorm.DB, data *store.EpochData) error {
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

				contract, _, err := ls.cs.AddContractIfAbsent(v.Address.MustGetBase32Address())
				if err != nil {
					return err
				}

				log := NewAddressIndexedLog(&v, contract.ID, bn, receiptExt.LogExts[i])
				partition := ls.getPartitionByAddress(&v.Address, ls.partitions)
				partition2Logs[partition] = append(partition2Logs[partition], log)
			}
		}
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

func (ls *AddressIndexedLogStore) GetAddressIndexedLogs(filter AddressIndexedLogFilter) ([]types.Log, []*store.LogExtra, error) {
	// Normalzie log filter at first
	partition := ls.getPartitionByAddress(&filter.Contract, ls.partitions)
	tableName := ls.getPartitionedTableName(&ls.model, partition)
	ok, err := filter.normalize(ls.cs, tableName)
	if err != nil {
		return nil, nil, err
	}

	if !ok {
		return emptyLogs, emptyLogExts, nil
	}

	// Check the number of returned logs
	if err = filter.ValidateCount(ls.db); err != nil {
		return nil, nil, err
	}

	// Query database
	db := filter.Apply(ls.db)

	var addrIndexedLogs []AddressIndexedLog
	if err := db.Find(&addrIndexedLogs).Error; err != nil {
		return nil, nil, err
	}

	// Convert to RPC data type
	logs := make([]types.Log, 0, len(addrIndexedLogs))
	exts := make([]*store.LogExtra, 0, len(addrIndexedLogs))

	for i := len(addrIndexedLogs) - 1; i >= 0; i-- {
		log, ext, err := addrIndexedLogs[i].ToRpcLog(ls.cs)
		if err != nil {
			return nil, nil, err
		}

		logs = append(logs, *log)
		exts = append(exts, ext)
	}

	return logs, exts, nil
}

// TODO supports repartition with consistent hashing and data migration.
