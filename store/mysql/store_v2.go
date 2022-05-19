package mysql

import (
	"context"
	"sort"

	"github.com/conflux-chain/conflux-infura/metrics"
	"github.com/conflux-chain/conflux-infura/store"
	citypes "github.com/conflux-chain/conflux-infura/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// TODO: ensure MysqlStore implements Store interface
// var _ store.Store = (*MysqlStoreV2)(nil)

type MysqlStoreV2 struct {
	*baseStore
	ts   *txStore
	bs   *blockStore
	ls   *logStoreV2
	ails *AddressIndexedLogStore
	ess  *EpochStatStore
	cfs  *confStore
	us   *UserStore
	cs   *ContractStore
	ebms *epochBlockMapStore

	// config
	config *Config
	// store chaindata disabler
	disabler store.StoreDisabler
}

func mustNewStoreV2(db *gorm.DB, config *Config, option StoreOption) *MysqlStoreV2 {
	cs := NewContractStore(db)
	ebms := newEpochBlockMapStore(db)

	ms := MysqlStoreV2{
		baseStore: newBaseStore(db),
		ts:        newTxStore(db),
		bs:        newBlockStore(db),
		ls:        newLogStoreV2(db, cs, ebms),
		ails:      NewAddressIndexedLogStore(db, cs, config.AddressIndexedLogPartitions),
		ess:       NewEpochStatStore(db),
		cfs:       newConfStore(db),
		us:        newUserStore(db),
		ebms:      ebms,
		cs:        cs,
		config:    config,
		disabler:  option.Disabler,
	}

	return &ms
}

func (ms *MysqlStoreV2) Push(data *store.EpochData) error {
	return ms.Pushn([]*store.EpochData{data})
}

func (ms *MysqlStoreV2) Pushn(dataSlice []*store.EpochData) error {
	if len(dataSlice) == 0 {
		return nil
	}

	storeMaxEpoch, ok, err := ms.ebms.MaxEpoch()
	if err != nil {
		return err
	}

	if !ok {
		storeMaxEpoch = citypes.EpochNumberNil
	}

	if err := store.RequireContinuous(dataSlice, storeMaxEpoch); err != nil {
		return err
	}

	updater := metrics.NewTimerUpdaterByName("infura/duration/store/mysql/write")
	defer updater.Update()

	// the log partition to write event logs
	var logPartition bnPartition

	if !ms.disabler.IsChainLogDisabled() {
		// add log contract address
		if ms.config.AddressIndexedLogEnabled {
			newAdded, err := ms.cs.AddContractByEpochData(dataSlice...)
			if err != nil {
				return errors.WithMessage(err, "failed to add contracts for specified epoch data slice")
			}

			// Note, even if failed to insert event logs afterward, no need to rollback the inserted contract records.
			if newAdded > 0 {
				logrus.WithField("count", newAdded).Debug("Succeeded to add new contract into database")
			}
		}

		// prepare for new log partitions if necessary before saving epoch data
		if logPartition, err = ms.ls.preparePartition(dataSlice); err != nil {
			return errors.WithMessage(err, "failed to prepare log partition")
		}
	}

	return ms.db.Transaction(func(dbTx *gorm.DB) error {
		if !ms.disabler.IsChainBlockDisabled() {
			// save blocks
			if err := ms.bs.Pushn(dbTx, dataSlice); err != nil {
				return errors.WithMessagef(err, "failed to save blocks")
			}
		}

		skipTxn := ms.disabler.IsChainTxnDisabled()
		skipRcpt := ms.disabler.IsChainReceiptDisabled()
		if !skipRcpt || !skipTxn {
			// save transactions or receipts
			if err := ms.ts.Pushn(dbTx, dataSlice, skipTxn, skipRcpt); err != nil {
				return errors.WithMessage(err, "failed to save transactions")
			}
		}

		if !ms.disabler.IsChainLogDisabled() {
			if ms.config.AddressIndexedLogEnabled {
				// save address indexed event logs
				for _, data := range dataSlice {
					if err := ms.ails.AddAddressIndexedLogs(dbTx, data); err != nil {
						return errors.WithMessage(err, "failed to save address indexed event logs")
					}

				}
			}

			// save event logs
			if err := ms.ls.Pushn(dbTx, dataSlice, logPartition); err != nil {
				return errors.WithMessage(err, "failed to save event logs")
			}

		}

		// save epoch to block mapping data
		return ms.ebms.Pushn(dbTx, dataSlice)
	})
}

// Popn pops multiple epoch data from database.
func (ms *MysqlStoreV2) Popn(epochUntil uint64) error {
	maxEpoch, ok, err := ms.ebms.MaxEpoch()
	if err != nil {
		return errors.WithMessage(err, "failed to get max epoch")
	}

	if !ok || epochUntil > maxEpoch { // no data in database or popped beyond the max epoch
		return nil
	}

	updater := metrics.NewTimerUpdaterByName("infura/store/mysql/delete")
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
			}

			// pop event logs
			if err := ms.ls.Popn(dbTx, epochUntil); err != nil {
				return errors.WithMessage(err, "failed to pop event logs")
			}
		}

		// remove epoch to block mapping data
		if err := ms.ebms.Remove(dbTx, epochUntil, maxEpoch); err != nil {
			return errors.WithMessage(err, "failed to remove epoch to block mapping data")
		}

		// pop is always due to pivot chain switch, update reorg version too
		return ms.cfs.createOrUpdateReorgVersion(dbTx)
	})
}

func (ms *MysqlStoreV2) GetLogs(ctx context.Context, storeFilter store.LogFilterV2) ([]*store.LogV2, error) {
	updater := metrics.NewTimerUpdaterByName("infura/store/mysql/getlogs")
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
		contract, exists, err := ms.cs.GetContractByAddress(addr)
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
			ContractId: contract.ID,
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
	go ms.ls.SchedulePrune(ms.config.MaxBnRangedArchiveLogPartitions)
}
