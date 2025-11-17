package mysql

import (
	"context"
	"fmt"
	"math"

	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/types"
	"github.com/Conflux-Chain/confura/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	// threshold count of event logs for contract to be regarded as big contract.
	thresholdBigContractLogCount = 100_000

	// default batch size for migrating logs from address indexed table to contract specified log table
	defaultBatchSizeForLogMigrating = 1_000
)

// contractLog event logs for specified contract
type contractLog struct {
	ID          uint64
	ContractID  uint64 `gorm:"-"` // ignored
	BlockNumber uint64 `gorm:"column:bn;not null;index:idx_bn"`
	Epoch       uint64 `gorm:"not null"`
	Topic0      string `gorm:"size:66;not null"`
	Topic1      string `gorm:"size:66"`
	Topic2      string `gorm:"size:66"`
	Topic3      string `gorm:"size:66"`
	LogIndex    uint64 `gorm:"not null"`
	Extra       []byte `gorm:"type:mediumText"` // extension json field
}

func (cl contractLog) TableName() string {
	return fmt.Sprintf("clogs_%d", cl.ContractID)
}

// bigContractLogStore partitioned store for big contract which has considerable amount of
// event logs.
type bigContractLogStore struct {
	*bnPartitionedStore
	cs   *ContractStore
	ebms *epochBlockMapStore
	ails *AddressIndexedLogStore
	// notify channel for new bn partition created
	bnPartitionNotifyChan chan<- *bnPartition
}

func newBigContractLogStore(
	db *gorm.DB,
	cs *ContractStore,
	ebms *epochBlockMapStore,
	ails *AddressIndexedLogStore,
	notifyChan chan<- *bnPartition,
) *bigContractLogStore {
	return &bigContractLogStore{
		bnPartitionedStore:    newBnPartitionedStore(db),
		bnPartitionNotifyChan: notifyChan, cs: cs, ebms: ebms, ails: ails,
	}
}

// preparePartitions create new contract log partitions for the big contract if necessary.
// Also migrates event logs from address indexed table to separate contract specified log table
// for the initial partitioning.
func (bcls *bigContractLogStore) preparePartitions(dataSlice []*store.EpochData) (map[uint64]bnPartition, error) {
	contractAddrs := extractUniqueContractAddresses(dataSlice...)
	contract2BnPartitions := make(map[uint64]bnPartition)

	for caddr := range contractAddrs {
		cid, _, err := bcls.cs.GetContractIdByAddress(caddr)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to get contract id by addr")
		}

		contract, ok, err := bcls.cs.GetContractById(cid)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to get contract by id")
		}

		if !ok {
			return nil, errors.WithMessage(store.ErrNotFound, "contract not found")
		}

		if contract.LogCount < thresholdBigContractLogCount {
			// not qualified for big contract
			continue
		}

		clEntity, clTabler := bcls.contractEntity(contract.ID), bcls.contractTabler(contract.ID)
		partition, newCreated, err := bcls.autoPartition(clEntity, clTabler, bnPartitionedLogVolumeSize)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to auto contract log partition")
		}

		if newCreated {
			partition.tabler = clTabler
			bcls.bnPartitionNotifyChan <- &partition
		}

		contract2BnPartitions[contract.ID] = partition

		// contract is considered as migrated already for the following cases:
		// 1. write partition is not the initial one;
		// 2. write partition is the initial one but has data already.
		if !partition.IsInitial() || partition.Count > 0 {
			continue
		}

		// Migrate event logs of new big contract to separate log table partition.
		// We assume the possibility of migration for more than one big contracts
		// at the same time is very small, otherwise the migration process might
		// collapse the sync progress.
		if err := bcls.migrate(contract, partition); err != nil {
			return nil, errors.WithMessage(err, "failed to migrate contract logs")
		}
	}

	return contract2BnPartitions, nil
}

// migrate migrates address indexed event logs for the big contract to separate log table partition.
func (bcls *bigContractLogStore) migrate(contract *Contract, partition bnPartition) error {
	aiTableName := bcls.ails.GetPartitionedTableName(contract.Address)

	clEntity, clTabler := bcls.contractEntity(contract.ID), bcls.contractTabler(contract.ID)
	clTableName := bcls.getPartitionedTableName(clTabler, partition.Index)

	return bcls.db.Transaction(func(dbTx *gorm.DB) error {
		var aiLogs []*AddressIndexedLog
		var bnMin, bnMax uint64

		aidb := dbTx.Table(aiTableName).Where("cid = ?", contract.ID)

		res := aidb.FindInBatches(&aiLogs, defaultBatchSizeForLogMigrating, func(tx *gorm.DB, batch int) error {
			deleteIds := make([]uint64, 0, len(aiLogs))
			clLogs := make([]*contractLog, 0, len(aiLogs))

			for _, aiLog := range aiLogs {
				// copy address indexed event log
				clog := (contractLog)(*aiLog)
				// clear primary id
				clog.ID = 0

				clLogs = append(clLogs, &clog)
				deleteIds = append(deleteIds, aiLog.ID)
			}

			// insert into separate contract log table
			if err := dbTx.Table(clTableName).Create(&clLogs).Error; err != nil {
				return errors.WithMessage(err, "failed to insert contract logs")
			}

			// delete from address indexed log table
			delRes := dbTx.Table(aiTableName).Where("id IN (?)", deleteIds).Delete(&contractLog{})
			if err := delRes.Error; err != nil {
				return errors.WithMessage(err, "failed to delete address indexed log")
			}

			if batch == 1 { // least block number of the first batch
				bnMin = aiLogs[0].BlockNumber
			}
			bnMax = aiLogs[len(aiLogs)-1].BlockNumber

			return nil
		})

		if err := res.Error; err != nil {
			return errors.WithMessage(err, "failed to batch get address indexed logs for migration")
		}

		// expand partition block number range
		if err := bcls.expandBnRange(dbTx, clEntity, int(partition.Index), 0, bnMax); err != nil {
			return errors.WithMessage(err, "failed to expand partition bn range")
		}

		// update separate contract log partition count
		err := bcls.deltaUpdateCount(dbTx, clEntity, int(partition.Index), int(res.RowsAffected))
		if err != nil {
			return errors.WithMessage(err, "failed to update contract log partition count")
		}

		logrus.WithFields(logrus.Fields{
			"contract":          contract,
			"aiTableName":       aiTableName,
			"clTableName":       clTableName,
			"bnMin":             bnMin,
			"bnMax":             bnMax,
			"totalMigratedLogs": res.RowsAffected,
		}).Info("Address indexed event logs migrated to big contract event logs table")

		return nil
	})
}

// contractEntity gets partition entity of contract logs
func (bcls *bigContractLogStore) contractEntity(cid uint64) string {
	return contractLog{ContractID: cid}.TableName()
}

// contractTabler get partition tabler of contract logs
func (bcls *bigContractLogStore) contractTabler(cid uint64) *contractLog {
	return &contractLog{ContractID: cid}
}

func (bcls *bigContractLogStore) Add(
	dbTx *gorm.DB, dataSlice []*store.EpochData, contract2BnPartitions map[uint64]bnPartition,
) error {
	bnMin, bnMax := uint64(math.MaxUint64), uint64(0)
	contract2Logs := make(map[uint64][]*contractLog, len(contract2BnPartitions))

	for _, data := range dataSlice {
		for _, block := range data.Blocks {
			bn := block.BlockNumber.ToInt().Uint64()
			bnMin, bnMax = min(bnMin, bn), max(bnMax, bn)

			for _, tx := range block.Transactions {
				receipt := data.Receipts[tx.Hash]

				// Skip transactions that unexecuted in block.
				if receipt == nil || !util.IsTxExecutedInBlock(&tx) {
					continue
				}

				var rcptExt *store.ReceiptExtra
				if len(data.ReceiptExts) > 0 {
					rcptExt = data.ReceiptExts[tx.Hash]
				}

				for k, log := range receipt.Logs {
					cid, _, err := bcls.cs.AddContractIfAbsent(log.Address.MustGetBase32Address())
					if err != nil {
						return errors.WithMessage(err, "failed to add contract")
					}

					// only collect big contract event logs
					if _, ok := contract2BnPartitions[cid]; !ok {
						continue
					}

					var logExt *store.LogExtra
					if rcptExt != nil && k < len(rcptExt.LogExts) {
						logExt = rcptExt.LogExts[k]
					}

					log := store.ParseCfxLog(&log, cid, bn, logExt)
					contract2Logs[cid] = append(contract2Logs[cid], (*contractLog)(log))
				}
			}
		}
	}

	for cid, partition := range contract2BnPartitions {
		clEntity, clTabler := bcls.contractEntity(cid), bcls.contractTabler(cid)

		// update block range for contract log partition router
		err := bcls.expandBnRange(dbTx, clEntity, int(partition.Index), bnMin, bnMax)
		if err != nil {
			return errors.WithMessage(err, "failed to expand partition bn range")
		}

		logs := contract2Logs[cid]
		if len(logs) == 0 {
			continue
		}

		tblName := bcls.getPartitionedTableName(clTabler, partition.Index)
		err = dbTx.Table(tblName).Create(logs).Error
		if err != nil {
			return err
		}

		// update partition data count
		err = bcls.deltaUpdateCount(dbTx, clEntity, int(partition.Index), len(logs))
		if err != nil {
			return errors.WithMessage(err, "failed to delta update partition size")
		}

		// Update contract statistics (log count and latest updated epoch).
		latestUpdateEpoch := logs[len(logs)-1].Epoch
		if err := bcls.cs.UpdateContractStats(dbTx, cid, len(logs), latestUpdateEpoch); err != nil {
			return errors.WithMessage(err, "failed to update contract statistics")
		}
	}

	return nil
}

func (bcls *bigContractLogStore) Popn(dbTx *gorm.DB, epochUntil uint64) error {
	contracts, err := bcls.cs.GetUpdatedContractsSinceEpoch(epochUntil)
	if err != nil {
		return errors.WithMessage(err, "failed to get updated contracts since start epoch")
	}

	if len(contracts) == 0 { // no possible contracts found
		return nil
	}

	e2bmap, ok, err := bcls.ebms.CeilBlockMapping(epochUntil)
	if err != nil {
		return errors.WithMessagef(err, "failed to get block mapping for epoch %v", epochUntil)
	}

	if !ok { // no block mapping found for epoch
		return errors.Errorf("no block mapping found for epoch %v", epochUntil)
	}

	// delete event logs for all possible contracts.
	for _, contract := range contracts {
		contractEntity := bcls.contractEntity(contract.ID)
		contractTabler := bcls.contractTabler(contract.ID)

		partitions, existed, err := bcls.shrinkBnRange(dbTx, contractEntity, e2bmap.BnMin)
		if err != nil {
			return errors.WithMessage(err, "failed to shrink partition bn range")
		}

		if !existed { // no specified log partition found for the contract
			continue
		}

		totalRowsAffected := int64(0)

		for i := len(partitions) - 1; i >= 0; i-- {
			partition := partitions[i]
			tblName := bcls.getPartitionedTableName(contractTabler, partition.Index)

			res := dbTx.Table(tblName).Where("bn >= ?", e2bmap.BnMin).Delete(&contractLog{})
			if res.Error != nil {
				return res.Error
			}

			// update partition data count
			err = bcls.deltaUpdateCount(dbTx, contractEntity, int(partition.Index), -int(res.RowsAffected))
			if err != nil {
				return errors.WithMessage(err, "failed to delta update partition size")
			}

			totalRowsAffected += res.RowsAffected
		}

		// update contract statistics (log count and latest updated epoch).
		if err := bcls.cs.UpdateContractStats(dbTx, contract.ID, int(-totalRowsAffected), epochUntil); err != nil {
			return errors.WithMessage(err, "failed to update contract statistics")
		}
	}

	return nil
}

// IsBigContract check if the contract is big contract or not.
func (bcls *bigContractLogStore) IsBigContract(cid uint64) (bool, error) {
	contractEntity := bcls.contractEntity(cid)
	partition, existed, err := bcls.oldestPartition(contractEntity)
	if err != nil {
		return false, errors.WithMessage(err, "failed to check contract partition")
	}

	if !existed {
		return false, nil
	}

	// regarded as big contract with the following two cases:
	// 1. oldest partition is not the initial one;
	// 2. oldest partition is the initial one and has data on it.
	return !partition.IsInitial() || partition.Count > 0, nil
}

// GetContractLogs get contract logs for the specified filter.
func (bcls *bigContractLogStore) GetContractLogs(
	ctx context.Context, cid uint64, storeFilter store.LogFilter,
) ([]*store.Log, error) {
	contractEntity := bcls.contractEntity(cid)
	partitions, _, err := bcls.searchPartitions(
		contractEntity, types.RangeUint64{
			From: storeFilter.BlockFrom,
			To:   storeFilter.BlockTo,
		},
	)

	if err != nil {
		return nil, errors.WithMessage(err, "failed to search partitions")
	}

	filter := LogFilter{
		BlockFrom: storeFilter.BlockFrom,
		BlockTo:   storeFilter.BlockTo,
		Topics:    storeFilter.Topics,
	}

	var result []*store.Log
	for _, partition := range partitions {
		// check timeout before query
		select {
		case <-ctx.Done():
			return nil, store.ErrGetLogsTimeout
		default:
		}

		logs, err := bcls.GetContractBnPartitionedLogs(ctx, cid, filter, *partition)
		if err != nil {
			return nil, err
		}

		// convert to common store log
		for _, v := range logs {
			// fill contract id since it's not persisted in db
			v.ContractID = cid
			result = append(result, (*store.Log)(v))
		}

		// check log count
		if store.IsBoundChecksEnabled(ctx) && len(result) > int(store.MaxLogLimit) {
			return nil, newSuggestedFilterResultSetTooLargeError(&storeFilter, result, true)
		}
	}

	return result, nil
}

// GetContractBnPartitionedLogs returns contract event logs for the log filter from
// specified table partition ranged by block number.
func (bcls *bigContractLogStore) GetContractBnPartitionedLogs(
	ctx context.Context, cid uint64, filter LogFilter, partition bnPartition,
) ([]*contractLog, error) {
	contractTabler := bcls.contractTabler(cid)
	filter.TableName = bcls.getPartitionedTableName(contractTabler, partition.Index)

	var res []*contractLog
	err := filter.find(ctx, bcls.db, &res)

	return res, err
}
