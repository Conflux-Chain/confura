package mysql

import (
	"github.com/conflux-chain/conflux-infura/store"
	"gorm.io/gorm"
)

// TODO: ensure MysqlStore implements Store interface
// var _ store.Store = (*MysqlStoreV2)(nil)

type MysqlStoreV2 struct {
	*baseStore
	ts   *txStore
	bs   *blockStore
	ls   *logStore
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
		ls:        newLogStore(db),
		ails:      NewAddressIndexedLogStore(db, cs, config.AddressIndexedLogPartitions),
		ess:       NewEpochStatStore(db),
		cfs:       newConfStore(db),
		us:        newUserStore(db),
		ebms:      newEochBlockMapStore(db),
		cs:        cs,
		config:    config,
		disabler:  option.Disabler,
	}

	return &ms
}
