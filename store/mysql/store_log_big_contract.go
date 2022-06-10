package mysql

import (
	"context"
	"fmt"

	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

const (
	// threshold count of event logs for contract to be regarded as big contract.
	thresholdBigContractLogCount = 100_000
)

// contractLog event logs for specified contract
type contractLog struct {
	ID          uint64
	ContractID  uint64 `gorm:"column:-"` // ignored
	BlockNumber uint64 `gorm:"column:bn;not null;index:idx_bn"`
	Epoch       uint64 `gorm:"not null"`
	Topic0      string `gorm:"size:66;not null"`
	Topic1      string `gorm:"size:66"`
	Topic2      string `gorm:"size:66"`
	Topic3      string `gorm:"size:66"`
	LogIndex    uint64 `gorm:"not null"`
	Extra       []byte `gorm:"type:mediumText"` // extra data in JSON format
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
}

func newBigContractLogStore(
	db *gorm.DB, cs *ContractStore, ebms *epochBlockMapStore, ails *AddressIndexedLogStore,
) *bigContractLogStore {
	return &bigContractLogStore{
		bnPartitionedStore: newBnPartitionedStore(db), cs: cs, ebms: ebms, ails: ails,
	}
}

// preparePartition create new contract log partitions for the big contract if necessary.
// Also migrates event logs form address indexed table to separate contract specified log table for the initial partitioning.
func (bcls *bigContractLogStore) preparePartitions(dataSlice []*store.EpochData) (map[uint64]bnPartition, error) {
	contractAddrs := extractContractAddressesOfEpochData(dataSlice...)
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
		partition, _, err := bcls.autoPartition(clEntity, clTabler, bnPartitionedLogVolumeSize)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to auto contract log partition")
		}

		contract2BnPartitions[contract.ID] = partition

		// contract is considered as migrated already for the following cases:
		// 1. write partition is not the initial one;
		// 2. write partition is the initial one but has data already.
		if !partition.IsInitial() || partition.Count > 0 {
			continue
		}

		// Migrate event logs of new big contract to seperate log table partition.
		// We assume the possibility of migration for more than one big contracts
		// at the same time is very small, otherwise the migration process might
		// collapse the sync progress.
		if err := bcls.migrate(contract, partition); err != nil {
			return nil, errors.WithMessage(err, "failed to migrate contract logs")
		}
	}

	return contract2BnPartitions, nil
}

// migrate migrates address indexed event logs for the big contract to seperate log table partition.
func (bcls *bigContractLogStore) migrate(contract *Contract, partition bnPartition) error {
	aiTableName := bcls.ails.GetPartitionedTableName(contract.Address)

	clEntity, clTabler := bcls.contractEntity(contract.ID), bcls.contractTabler(contract.ID)
	clTableName := bcls.getPartitionedTableName(clTabler, partition.Index)

	return bcls.db.Transaction(func(dbTx *gorm.DB) error {
		var aiLogs []*AddressIndexedLog

		aidb := dbTx.Table(aiTableName).Where("cid = ?", contract.ID)
		res := aidb.FindInBatches(&aiLogs, defaultBatchSizeLogInsert, func(tx *gorm.DB, batch int) error {
			clLogs := make([]*contractLog, 0, len(aiLogs))
			for _, aiLog := range aiLogs {
				clLogs = append(clLogs, (*contractLog)(aiLog))
			}

			// insert into seperate contract log table
			if err := dbTx.Table(clTableName).Create(&clLogs).Error; err != nil {
				return errors.WithMessage(err, "failed to insert contract logs")
			}

			// delete from address indexed log table
			lastPrimaryId := aiLogs[len(aiLogs)-1].ID
			delRes := dbTx.Table(aiTableName).Where("id <= ?", lastPrimaryId).Delete(&contractLog{})
			if err := delRes.Error; err != nil {
				return errors.WithMessage(err, "failed to delete address indexed log")
			}

			return nil
		})

		if err := res.Error; err != nil {
			return errors.WithMessage(err, "failed to batch get address indexed logs for migration")
		}

		// update seperate contract log partition count
		err := bcls.deltaUpdateCount(dbTx, clEntity, int(partition.Index), int(res.RowsAffected))
		if err != nil {
			return errors.WithMessage(err, "failed to update contract log partition count")
		}

		return nil
	})
}

// contractEntity gets partition entity of contract logs
func (bcls *bigContractLogStore) contractEntity(cid uint64) string {
	return contractLog{ContractID: cid}.TableName()
}

// contractEntity get partition tabler of contract logs
func (bcls *bigContractLogStore) contractTabler(cid uint64) *contractLog {
	return &contractLog{ContractID: cid}
}

func (bcls *bigContractLogStore) Add(
	dbTx *gorm.DB, dataSlice []*store.EpochData, contract2BnPartitions map[uint64]bnPartition,
) error {
	contract2Logs := make(map[uint64][]*contractLog, len(contract2BnPartitions))

	for _, data := range dataSlice {
		for _, block := range data.Blocks {
			bn := block.BlockNumber.ToInt().Uint64()

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

	bnMin := dataSlice[0].Blocks[0].BlockNumber.ToInt().Uint64()
	bnMax := dataSlice[len(dataSlice)-1].GetPivotBlock().BlockNumber.ToInt().Uint64()

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
		err = dbTx.Table(tblName).CreateInBatches(logs, defaultBatchSizeLogInsert).Error
		if err != nil {
			return err
		}

		// update partition data count
		err = bcls.deltaUpdateCount(dbTx, clEntity, int(partition.Index), len(logs))
		if err != nil {
			return errors.WithMessage(err, "failed to delta update partition size")
		}

		// Update contract statistics (log count and lastest updated epoch).
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

	bn, ok, err := bcls.ebms.BlockRange(epochUntil)
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

		partitions, existed, err := bcls.shrinkBnRange(dbTx, contractEntity, bn.From)
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

			res := dbTx.Table(tblName).Where("bn >= ?", bn.From).Delete(&contractLog{})
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

		// update contract statistics (log count and lastest updated epoch).
		if err := bcls.cs.UpdateContractStats(dbTx, contract.ID, int(-totalRowsAffected), epochUntil); err != nil {
			return errors.WithMessage(err, "failed to update contract statistics")
		}
	}

	return nil
}

func (bcls *bigContractLogStore) GetLogs(ctx context.Context, storeFilter store.LogFilterV2) ([]*store.LogV2, error) {
	// TODO:
	// 1. get event logs like storeLogV2
	// 2. contractLog.ContractId must be filled since it's not persisted in db.
	return nil, store.ErrUnsupported
}

func (bcls *bigContractLogStore) SchedulePrune(maxArchivePartitions uint32) {
	// TODO:
	// 1. prune for existed big contract should be scheduled at first;
	// 2. prune for fresh new big contract should be scheduled right after migration.
	panic(store.ErrUnsupported)
}
