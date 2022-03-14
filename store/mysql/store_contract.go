package mysql

import (
	"github.com/conflux-chain/conflux-infura/store"
	lru "github.com/hashicorp/golang-lru"
	"gorm.io/gorm"
)

const defaultContractCacheSize = 4096

type Contract struct {
	ID      uint64
	Address string `gorm:"size:64;not null;unique"`
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

func (cs *ContractStore) GetContractById(id uint64) (*Contract, bool, error) {
	// check cache at first
	if val, ok := cs.cacheById.Get(id); ok {
		return val.(*Contract), true, nil
	}

	var contract Contract
	exists, err := cs.exists(&contract, "id = ?", id)
	if err == nil && exists {
		cs.updateCache(&contract)
	}

	return &contract, exists, err
}

func (cs *ContractStore) GetContractByAddress(address string) (*Contract, bool, error) {
	// check cache at first
	if val, ok := cs.cacheByAddress.Get(address); ok {
		return val.(*Contract), true, nil
	}

	var contract Contract
	exists, err := cs.exists(&contract, "address = ?", address)
	if err == nil && exists {
		cs.updateCache(&contract)
	}

	return &contract, exists, err
}

func (cs *ContractStore) AddContractIfAbsent(address string) (*Contract, bool, error) {
	existing, ok, err := cs.GetContractByAddress(address)
	if err != nil {
		return nil, false, err
	}

	if ok {
		return existing, false, nil
	}

	// Thread unsafe
	contract := Contract{
		Address: address,
	}

	if err := cs.db.Create(&contract).Error; err != nil {
		return nil, false, err
	}

	cs.updateCache(&contract)

	return &contract, true, nil
}

// AddContract adds contract in batch and return the number of new added contracts.
func (cs *ContractStore) AddContract(contracts map[string]bool) (int, error) {
	var newContracts []Contract

	// ignore contracts that already exists
	for addr := range contracts {
		_, ok, err := cs.GetContractByAddress(addr)
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

	// update cache
	for i := range newContracts {
		cs.updateCache(&newContracts[i])
	}

	return len(newContracts), nil
}

// AddContract adds contract for the specified epoch data slice and return the number of new added contracts.
func (cs *ContractStore) AddContractByEpochData(slice ...*store.EpochData) (int, error) {
	contracts := make(map[string]bool)

	for _, data := range slice {
		for _, receipt := range data.Receipts {
			for i := range receipt.Logs {
				addr := receipt.Logs[i].Address.String()
				contracts[addr] = true
			}
		}
	}

	return cs.AddContract(contracts)
}
