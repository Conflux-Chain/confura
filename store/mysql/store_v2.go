package mysql

import (
	"context"
	"sort"

	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/conflux-chain/conflux-infura/util/metrics"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type MysqlStoreV2 struct {
	*baseStore
	*epochBlockMapStore
	ts   *txStore
	bs   *blockStore
	ls   *logStoreV2
	ails *AddressIndexedLogStore
	bcls *bigContractLogStore
	cfs  *confStore
	us   *UserStore
	cs   *ContractStore

	// config
	config *Config
	// store chaindata disabler
	disabler store.StoreDisabler
}

// NewStoreV2FromV1 adapts a v1 store to v2 store.
// TODO: deprecate this function once v2 is production ready.
func NewStoreV2FromV1(v1 *MysqlStore) *MysqlStoreV2 {
	bcls := newBigContractLogStore(v1.db, v1.cs, v1.epochBlockMapStore, v1.AddressIndexedLogStore)
	return &MysqlStoreV2{
		baseStore:          v1.baseStore,
		epochBlockMapStore: v1.epochBlockMapStore,
		ts:                 v1.txStore,
		bs:                 v1.blockStore,
		ls:                 newLogStoreV2(v1.db, v1.cs, v1.epochBlockMapStore),
		bcls:               bcls,
		ails:               v1.AddressIndexedLogStore,
		cfs:                v1.confStore,
		us:                 v1.UserStore,
		cs:                 v1.cs,
		config:             v1.config,
		disabler:           v1.disabler,
	}
}

func mustNewStoreV2(db *gorm.DB, config *Config, option StoreOption) *MysqlStoreV2 {
	cs := NewContractStore(db)
	ebms := newEpochBlockMapStore(db, config)
	ails := NewAddressIndexedLogStore(db, cs, config.AddressIndexedLogPartitions)

	return &MysqlStoreV2{
		baseStore:          newBaseStore(db),
		epochBlockMapStore: ebms,
		ts:                 newTxStore(db),
		bs:                 newBlockStore(db),
		ls:                 newLogStoreV2(db, cs, ebms),
		bcls:               newBigContractLogStore(db, cs, ebms, ails),
		ails:               ails,
		cfs:                newConfStore(db),
		us:                 newUserStore(db),
		cs:                 cs,
		config:             config,
		disabler:           option.Disabler,
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

	return ms.db.Transaction(func(dbTx *gorm.DB) error {
		if !ms.disabler.IsChainBlockDisabled() {
			// save blocks
			if err := ms.bs.Add(dbTx, dataSlice); err != nil {
				return errors.WithMessagef(err, "failed to save blocks")
			}
		}

		skipTxn := ms.disabler.IsChainTxnDisabled()
		skipRcpt := ms.disabler.IsChainReceiptDisabled()
		if !skipRcpt || !skipTxn {
			// save transactions or receipts
			if err := ms.ts.Add(dbTx, dataSlice, skipTxn, skipRcpt); err != nil {
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

	return ms.db.Transaction(func(dbTx *gorm.DB) error {
		if !ms.disabler.IsChainBlockDisabled() {
			// remove blocks
			if err := ms.bs.Remove(dbTx, epochUntil, maxEpoch); err != nil {
				return errors.WithMessage(err, "failed to remove blocks")
			}
		}

		skipTxn := ms.disabler.IsChainTxnDisabled()
		skipRcpt := ms.disabler.IsChainReceiptDisabled()
		if !skipRcpt || !skipTxn {
			// remove transactions or receipts
			if err := ms.ts.Remove(dbTx, epochUntil, maxEpoch); err != nil {
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
		return ms.cfs.createOrUpdateReorgVersion(dbTx)
	})
}

func (ms *MysqlStoreV2) GetLogs(ctx context.Context, storeFilter store.LogFilterV2) ([]*store.LogV2, error) {
	updater := metrics.Registry.Store.GetLogs()
	defer updater.Update()

	contracts := storeFilter.Contracts.ToSlice()

	// if address not specified, query event logs from block number partitioned tables.
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

		// check timeout before query
		select {
		case <-ctx.Done():
			return nil, store.ErrGetLogsTimeout
		default:
		}

		// query address indexed logs
		addrFilter := AddressIndexedLogFilter{
			LogFilter:  filter,
			ContractId: cid,
		}

		// TODO: if contract is big contract, find the event logs within seperate table.

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
	go ms.ls.SchedulePrune(ms.config.MaxBnRangedArchiveLogPartitions)

	// TODO: schedule to prune big contract log partitions
}
