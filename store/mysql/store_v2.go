package mysql

import (
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

	ms := MysqlStoreV2{
		baseStore: newBaseStore(db),
		ts:        newTxStore(db),
		bs:        newBlockStore(db),
		ls:        newLogStoreV2(db, cs),
		ails:      NewAddressIndexedLogStore(db, cs, config.AddressIndexedLogPartitions),
		ess:       NewEpochStatStore(db),
		cfs:       newConfStore(db),
		us:        newUserStore(db),
		ebms:      newEpochBlockMapStore(db),
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
