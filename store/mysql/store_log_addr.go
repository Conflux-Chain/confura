package mysql

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util"
	"github.com/pkg/errors"
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
	Extra       []byte `gorm:"type:mediumText"` // extention json field
}

func (AddressIndexedLog) TableName() string {
	return "addr_logs"
}

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
func (ls *AddressIndexedLogStore) convertToPartitionedLogs(
	data *store.EpochData, bigContractIds map[uint64]bool,
) (map[uint32][]*AddressIndexedLog, map[uint64]int, error) {
	contract2LogCount := make(map[uint64]int)
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
				// This could happen if there are no event logs for this transaction.
				continue
			}

			receiptExt := data.ReceiptExts[tx.Hash]

			for i, v := range receipt.Logs {
				cid, _, err := ls.cs.AddContractIfAbsent(v.Address.MustGetBase32Address())
				if err != nil {
					return nil, nil, err
				}

				// ignore event logs of big contracts, which will be stored in separate tables.
				if bigContractIds[cid] {
					continue
				}

				var logext *store.LogExtra
				if receiptExt != nil {
					logext = receiptExt.LogExts[i]
				}

				log := store.ParseCfxLog(&v, cid, bn, logext)
				partition := ls.getPartitionByAddress(v.Address.MustGetBase32Address())
				partition2Logs[partition] = append(partition2Logs[partition], (*AddressIndexedLog)(log))

				contract2LogCount[cid]++
			}
		}
	}

	return partition2Logs, contract2LogCount, nil
}

// Add inserts event logs from a batch of epochs into partitioned tables, while ignoring logs from big contracts.
func (ls *AddressIndexedLogStore) Add(dbTx *gorm.DB, dataSlice []*store.EpochData, bigContractIds map[uint64]bool) error {
	var (
		allContract2LogCount      = make(map[uint64]int)
		allContract2UpdatedEpochs = make(map[uint64]uint64)
		allPartition2Logs         = make(map[uint32][]*AddressIndexedLog)
	)

	// Merge all event logs of different epochs partitioned by contract address together for later bulk insert for performance.
	//
	// `allPartition2Logs` is used to store all event logs of different epochs partitioned by contract address.
	// `allContract2LogCount` is used to store the total count of event logs for each contract.
	// `allContract2UpdatedEpochs` is used to store the updated epoch for each contract.
	for _, data := range dataSlice {
		// Divides event logs into different partitions by address.
		partition2Logs, contract2LogCount, err := ls.convertToPartitionedLogs(data, bigContractIds)
		if err != nil {
			return errors.WithMessage(err, "failed to convert to partitioned logs")
		}

		// Merge all event logs of different epochs partitioned by contract address into `allPartition2Logs`
		for partition, logs := range partition2Logs {
			allPartition2Logs[partition] = append(allPartition2Logs[partition], logs...)
		}

		// Merge all count of event logs for each contract into `allContract2LogCount`
		for cid, logCount := range contract2LogCount {
			allContract2LogCount[cid] += logCount
		}

		// Update the updated epoch for each contract into `allContract2UpdatedEpochs`
		for cid := range contract2LogCount {
			allContract2UpdatedEpochs[cid] = data.Number
		}
	}

	// Insert address indexed logs into different partitions.
	for partition, logs := range allPartition2Logs {
		tableName := ls.getPartitionedTableName(&ls.model, partition)
		if err := dbTx.Table(tableName).Create(&logs).Error; err != nil {
			return err
		}
	}

	for cid, logCount := range allContract2LogCount {
		// Update contract statistics (log count and lastest updated epoch).
		latestUpdatedEpoch := allContract2UpdatedEpochs[cid]
		if err := ls.cs.UpdateContractStats(dbTx, cid, logCount, latestUpdatedEpoch); err != nil {
			return errors.WithMessage(err, "failed to update contract statistics")
		}
	}

	return nil
}

// DeleteAddressIndexedLogs removes event logs of specified epoch number range.
//
// Generally, this is used when pivot chain switched for confirmed blocks.
func (ls *AddressIndexedLogStore) DeleteAddressIndexedLogs(dbTx *gorm.DB, epochFrom, epochTo uint64) error {
	contracts, err := ls.cs.GetUpdatedContractsSinceEpoch(epochFrom)
	if err != nil {
		return errors.WithMessage(err, "failed to get updated contracts since start epoch")
	}

	if len(contracts) == 0 {
		return nil
	}

	// Delete logs for all possible contracts in batches.
	for _, contract := range contracts {
		partition := ls.getPartitionByAddress(contract.Address)
		tableName := ls.getPartitionedTableName(&ls.model, partition)

		sql := fmt.Sprintf("DELETE FROM %v WHERE epoch BETWEEN ? AND ?", tableName)
		res := dbTx.Exec(sql, epochFrom, epochTo)
		if err := res.Error; err != nil {
			return err
		}

		// Update contract statistics (log count and lastest updated epoch).
		// A rough estimation of the number of logs deleted from the contract, not accounting for other contracts in the same partition
		// since the accuracy is not critical and it happens rarely.
		if err := ls.cs.UpdateContractStats(dbTx, contract.ID, int(-res.RowsAffected), epochFrom); err != nil {
			return errors.WithMessage(err, "failed to update contract statistics")
		}
	}

	return nil
}

// GetAddressIndexedLogs returns event logs for the specified filter.
func (ls *AddressIndexedLogStore) GetAddressIndexedLogs(
	ctx context.Context,
	filter AddressIndexedLogFilter,
	contract string,
) ([]*AddressIndexedLog, error) {
	filter.TableName = ls.GetPartitionedTableName(contract)
	return filter.Find(ctx, ls.db)
}

// GetPartitionedTableName returns partitioned table name with specified
// contract address hashed partition index
func (ls *AddressIndexedLogStore) GetPartitionedTableName(contract string) string {
	partition := ls.getPartitionByAddress(contract)
	return ls.getPartitionedTableName(&ls.model, partition)
}
