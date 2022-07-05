package mysql

import (
	"context"
	"io"
	"sort"

	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util/metrics"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

var (
	_ store.Readable      = (*MysqlStoreV2)(nil)
	_ store.StackOperable = (*MysqlStoreV2)(nil)
	_ store.Configurable  = (*MysqlStoreV2)(nil)
	_ io.Closer           = (*MysqlStoreV2)(nil)
)

type StoreOption struct {
	Disabler store.StoreDisabler
}

type MysqlStoreV2 struct {
	*baseStore
	*epochBlockMapStore
	*txStore
	*blockStore
	*confStore
	*UserStore
	ls   *logStoreV2
	ails *AddressIndexedLogStore
	bcls *bigContractLogStore
	cs   *ContractStore

	// config
	config *Config
	// store chaindata disabler
	disabler store.StoreDisabler
	// store pruner
	pruner *storePruner
}

func mustNewStoreV2(db *gorm.DB, config *Config, option StoreOption) *MysqlStoreV2 {
	pruner := newStorePruner(db)
	cs := NewContractStore(db)
	ebms := newEpochBlockMapStore(db, config)
	ails := NewAddressIndexedLogStore(db, cs, config.AddressIndexedLogPartitions)

	return &MysqlStoreV2{
		baseStore:          newBaseStore(db),
		epochBlockMapStore: ebms,
		txStore:            newTxStore(db),
		blockStore:         newBlockStore(db),
		confStore:          newConfStore(db),
		UserStore:          newUserStore(db),
		ls:                 newLogStoreV2(db, cs, ebms, pruner.newBnPartitionObsChan),
		bcls:               newBigContractLogStore(db, cs, ebms, ails, pruner.newBnPartitionObsChan),
		ails:               ails,
		cs:                 cs,
		config:             config,
		disabler:           option.Disabler,
		pruner:             pruner,
	}
}

func (ms *MysqlStoreV2) Push(data *store.EpochData) error {
	return ms.Pushn([]*store.EpochData{data})
}

func (ms *MysqlStoreV2) Pushn(dataSlice []*store.EpochData) error {
	if len(dataSlice) == 0 {
		return nil
	}

	storeMaxEpoch, ok, err := ms.MaxEpoch()
	if err != nil {
		return err
	}

	if !ok {
		storeMaxEpoch = citypes.EpochNumberNil
	}

	if err := store.RequireContinuous(dataSlice, storeMaxEpoch); err != nil {
		return err
	}

	updater := metrics.Registry.Store.Push("mysql")
	defer updater.Update()

	// the log partition to write universal event logs
	var logPartition bnPartition
	// the log partition to write event logs for specified big contract
	var contract2BnPartitions map[uint64]bnPartition

	if !ms.disabler.IsChainLogDisabled() {
		// add log contract address
		if ms.config.AddressIndexedLogEnabled {
			// Note, even if failed to insert event logs afterward, no need to rollback the inserted contract records.
			_, err := ms.cs.AddContractByEpochData(dataSlice...)
			if err != nil {
				return errors.WithMessage(err, "failed to add contracts for specified epoch data slice")
			}

			// prepare for big contract log partitions if necessary
			contract2BnPartitions, err = ms.bcls.preparePartitions(dataSlice)
			if err != nil {
				return errors.WithMessage(err, "failed to prepare big contract log partitions")
			}
		}

		// prepare for new log partitions if necessary before saving epoch data
		if logPartition, err = ms.ls.preparePartition(dataSlice); err != nil {
			return errors.WithMessage(err, "failed to prepare log partition")
		}
	}

	// prepare epoch to block mapping table partition if necessary
	if ms.epochBlockMapStore.preparePartition(dataSlice) != nil {
		return errors.New("failed to prepare epoch block map partition")
	}

	return ms.baseStore.db.Transaction(func(dbTx *gorm.DB) error {
		if !ms.disabler.IsChainBlockDisabled() {
			// save blocks
			if err := ms.blockStore.Add(dbTx, dataSlice); err != nil {
				return errors.WithMessagef(err, "failed to save blocks")
			}
		}

		skipTxn := ms.disabler.IsChainTxnDisabled()
		skipRcpt := ms.disabler.IsChainReceiptDisabled()
		if !skipRcpt || !skipTxn {
			// save transactions or receipts
			if err := ms.txStore.Add(dbTx, dataSlice, skipTxn, skipRcpt); err != nil {
				return errors.WithMessage(err, "failed to save transactions")
			}
		}

		if !ms.disabler.IsChainLogDisabled() {
			if ms.config.AddressIndexedLogEnabled {
				bigContractIds := make(map[uint64]bool, len(contract2BnPartitions))
				for cid := range contract2BnPartitions {
					bigContractIds[cid] = true
				}

				// save address indexed event logs
				for _, data := range dataSlice {
					if err := ms.ails.AddAddressIndexedLogs(dbTx, data, bigContractIds); err != nil {
						return errors.WithMessage(err, "failed to save address indexed event logs")
					}
				}

				// save contract specified event logs
				if err := ms.bcls.Add(dbTx, dataSlice, contract2BnPartitions); err != nil {
					return errors.WithMessage(err, "failed to save big contract logs")
				}
			}

			// save event logs
			if err := ms.ls.Add(dbTx, dataSlice, logPartition); err != nil {
				return errors.WithMessage(err, "failed to save event logs")
			}
		}

		// save epoch to block mapping data
		return ms.epochBlockMapStore.Add(dbTx, dataSlice)
	})
}

// Popn pops multiple epoch data from database.
func (ms *MysqlStoreV2) Popn(epochUntil uint64) error {
	maxEpoch, ok, err := ms.MaxEpoch()
	if err != nil {
		return errors.WithMessage(err, "failed to get max epoch")
	}

	if !ok || epochUntil > maxEpoch { // no data in database or popped beyond the max epoch
		return nil
	}

	updater := metrics.Registry.Store.Pop("mysql")
	defer updater.Update()

	return ms.baseStore.db.Transaction(func(dbTx *gorm.DB) error {
		if !ms.disabler.IsChainBlockDisabled() {
			// remove blocks
			if err := ms.blockStore.Remove(dbTx, epochUntil, maxEpoch); err != nil {
				return errors.WithMessage(err, "failed to remove blocks")
			}
		}

		skipTxn := ms.disabler.IsChainTxnDisabled()
		skipRcpt := ms.disabler.IsChainReceiptDisabled()
		if !skipRcpt || !skipTxn {
			// remove transactions or receipts
			if err := ms.txStore.Remove(dbTx, epochUntil, maxEpoch); err != nil {
				return errors.WithMessage(err, "failed to remove transactions")
			}
		}

		if !ms.disabler.IsChainLogDisabled() {
			// remove address indexed event logs
			if ms.config.AddressIndexedLogEnabled {
				if err := ms.ails.DeleteAddressIndexedLogs(dbTx, epochUntil, maxEpoch); err != nil {
					return errors.WithMessage(err, "failed to remove address indexed event logs")
				}

				if err := ms.bcls.Popn(dbTx, epochUntil); err != nil {
					return errors.WithMessage(err, "failed to remove big contract logs")
				}
			}

			// pop universal event logs
			if err := ms.ls.Popn(dbTx, epochUntil); err != nil {
				return errors.WithMessage(err, "failed to remove universal event logs")
			}
		}

		// remove epoch to block mapping data
		if err := ms.epochBlockMapStore.Remove(dbTx, epochUntil, maxEpoch); err != nil {
			return errors.WithMessage(err, "failed to remove epoch to block mapping data")
		}

		// pop is always due to pivot chain switch, update reorg version too
		return ms.confStore.createOrUpdateReorgVersion(dbTx)
	})
}

func (ms *MysqlStoreV2) GetLogs(ctx context.Context, storeFilter store.LogFilterV2) ([]*store.LogV2, error) {
	updater := metrics.Registry.Store.GetLogs()
	defer updater.Update()

	contracts := storeFilter.Contracts.ToSlice()

	// if address not specified, query from universal event log table partition
	// ranged by block number.
	if len(contracts) == 0 {
		return ms.ls.GetLogs(ctx, storeFilter)
	}

	filter := LogFilter{
		BlockFrom: storeFilter.BlockFrom,
		BlockTo:   storeFilter.BlockTo,
		Topics:    storeFilter.Topics,
	}

	var result []*store.LogV2
	for _, addr := range contracts {
		// convert contract address to id
		cid, exists, err := ms.cs.GetContractIdByAddress(addr)
		if err != nil {
			return nil, err
		}

		if !exists {
			continue
		}

		// check if the contract is a big contract or not
		isBigContract, err := ms.bcls.IsBigContract(cid)
		if err != nil {
			return nil, err
		}

		// if the contract is a big contract, find the event logs from seperate table.
		if isBigContract {
			logs, err := ms.bcls.GetContractLogs(ctx, cid, storeFilter)
			if err != nil {
				return nil, err
			}

			result = append(result, logs...)

			// check log count
			if len(result) > int(store.MaxLogLimit) {
				return nil, store.ErrGetLogsResultSetTooLarge
			}

			continue
		}

		// check timeout before query
		select {
		case <-ctx.Done():
			return nil, store.ErrGetLogsTimeout
		default:
		}

		// query from address indexed logs
		addrFilter := AddressIndexedLogFilter{
			LogFilter:  filter,
			ContractId: cid,
		}

		logs, err := ms.ails.GetAddressIndexedLogs(addrFilter, addr)
		if err != nil {
			return nil, err
		}

		// convert to common store log
		for _, v := range logs {
			result = append(result, (*store.LogV2)(v))
		}

		// check log count
		if len(result) > int(store.MaxLogLimit) {
			return nil, store.ErrGetLogsResultSetTooLarge
		}
	}

	sort.Sort(store.LogSlice(result))

	return result, nil
}

// Prune prune data from db store.
func (ms *MysqlStoreV2) Prune() {
	go ms.pruner.schedulePrune(ms.config)
}
