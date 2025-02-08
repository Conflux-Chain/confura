package mysql

import (
	"github.com/Conflux-Chain/confura/store"
	lru "github.com/hashicorp/golang-lru"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const defaultContractCacheSize = 4096

type Contract struct {
	// id and address are immutable once created.
	ID      uint64 // contract id
	Address string `gorm:"size:64;not null;unique"`

	// num of persisted event logs
	LogCount int `gorm:"not null;default:0"`
	// latest updated epoch height
	LatestUpdatedEpoch uint64 `gorm:"index:index_lue;not null;default:0"`
}

func (Contract) TableName() string {
	return "contracts"
}

type ContractStore struct {
	*baseStore

	// generally, active contracts are limited
	cacheById      *lru.Cache // id => contract
	cacheByAddress *lru.Cache // address => contract
}

func NewContractStore(db *gorm.DB, cacheSize ...int) *ContractStore {
	size := defaultContractCacheSize
	if len(cacheSize) > 0 && cacheSize[0] > 0 {
		size = cacheSize[0]
	}

	cacheById, _ := lru.New(size)
	cacheByAddress, _ := lru.New(size)

	return &ContractStore{
		baseStore:      newBaseStore(db),
		cacheById:      cacheById,
		cacheByAddress: cacheByAddress,
	}
}

func (cs *ContractStore) updateCache(contract *Contract) {
	cs.cacheById.Add(contract.ID, contract)
	cs.cacheByAddress.Add(contract.Address, contract)
}

// GetContractAddressById gets contract address by id from cache or db if not found.
func (cs *ContractStore) GetContractAddressById(id uint64) (string, bool, error) {
	// check cache at first
	if val, ok := cs.cacheById.Get(id); ok {
		return val.(*Contract).Address, true, nil
	}

	// load from db
	contract, existed, err := cs.enforceCache("id = ?", id)
	if err == nil && existed {
		return contract.Address, existed, err
	}

	return "", false, err
}

// GetContractIdByAddress gets contract id by address from cache or db if not found.
func (cs *ContractStore) GetContractIdByAddress(address string) (uint64, bool, error) {
	// check cache at first
	if val, ok := cs.cacheByAddress.Get(address); ok {
		return val.(*Contract).ID, true, nil
	}

	// load from db
	contract, existed, err := cs.enforceCache("address = ?", address)
	if err == nil && existed {
		return contract.ID, existed, err
	}

	return 0, false, err
}

// GetContractById gets contract by id from db and also loads the cache.
//
// For contract `LogCount` and `LastUpdatedEpoch` fields, they are quite mutable
// while cache maybe rarely updated.
func (cs *ContractStore) GetContractById(cid uint64) (*Contract, bool, error) {
	return cs.enforceCache("id = ?", cid)
}

// AddContractIfAbsent add new contract if absent. Note, this method is thread unsafe.
// Generally, it is used by data sync thread.
func (cs *ContractStore) AddContractIfAbsent(address string) (uint64, bool, error) {
	cid, ok, err := cs.GetContractIdByAddress(address)
	if err != nil {
		return 0, false, err
	}

	if ok {
		return cid, false, nil
	}

	// Thread unsafe
	contract := Contract{
		Address: address,
	}

	if err := cs.db.Create(&contract).Error; err != nil {
		return 0, false, err
	}

	cs.updateCache(&contract)

	return contract.ID, true, nil
}

// AddContract adds absent contracts in batch and returns the number of new added contracts.
func (cs *ContractStore) AddContract(contracts map[string]bool) (int, error) {
	var newContracts []Contract

	// ignore contracts that already exists
	for addr := range contracts {
		_, ok, err := cs.GetContractIdByAddress(addr)
		if err != nil {
			return 0, err
		}

		if !ok {
			newContracts = append(newContracts, Contract{
				Address: addr,
			})
		}
	}

	if len(newContracts) == 0 {
		return 0, nil
	}

	// create in batch
	if err := cs.db.Create(&newContracts).Error; err != nil {
		return 0, err
	}

	logrus.WithField("newContracts", newContracts).Debug("Succeeded to add new contracts into db")

	// update cache
	for i := range newContracts {
		cs.updateCache(&newContracts[i])
	}

	return len(newContracts), nil
}

// AddContractByEpochData adds contract for the specified epoch data slice and returns the number of new added contracts.
func (cs *ContractStore) AddContractByEpochData(slice ...*store.EpochData) (int, error) {
	contracts := extractUniqueContractAddresses(slice...)
	return cs.AddContract(contracts)
}

// GetUpdatedContractsSinceEpoch gets all contracts that have been updated since the specified epoch.
func (cs *ContractStore) GetUpdatedContractsSinceEpoch(epoch uint64) ([]*Contract, error) {
	var contracts []*Contract
	if err := cs.db.Where("latest_updated_epoch >= ?", epoch).Find(&contracts).Error; err != nil {
		return nil, err
	}

	return contracts, nil
}

// UpdateContractStats updates statistics (log count/latest updated epoch) of the specified contract.
func (cs *ContractStore) UpdateContractStats(
	dbTx *gorm.DB, cid uint64, countDelta int, latestUpdatedEpoch uint64,
) error {
	updates := map[string]interface{}{
		"latest_updated_epoch": latestUpdatedEpoch,
	}

	if countDelta != 0 {
		if countDelta > 0 { // increment
			updates["log_count"] = gorm.Expr("log_count + ?", countDelta)
		} else { // decrement
			updates["log_count"] = gorm.Expr("GREATEST(0, CAST(log_count AS SIGNED) - ?)", -countDelta)
		}
	}

	return dbTx.Model(&Contract{}).Where("id = ?", cid).Updates(updates).Error
}

// enforceCache enforces to load contract cache from db with specified condition.
func (cs *ContractStore) enforceCache(whereQuery string, args ...interface{}) (*Contract, bool, error) {
	// Could improve when QPS is very high:
	// 1. Use mutex lock to allow only 1 thread to read from db.
	// 2. Cache non-existent address.
	var contract Contract
	exists, err := cs.exists(&contract, whereQuery, args...)

	if err == nil && exists {
		cs.updateCache(&contract)
	}

	return &contract, exists, err
}
