package store

import (
	"io"
	"strings"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
)

var (
	cfxStoreConfig storeConfig
	ethStoreConfig storeConfig
)

// Prunable is used to prune historical data.
type Prunable interface {
	GetBlockEpochRange() (uint64, uint64, error)
	GetTransactionEpochRange() (uint64, uint64, error)
	GetLogEpochRange() (uint64, uint64, error)

	GetNumBlocks() uint64
	GetNumTransactions() uint64
	GetNumLogs() uint64

	// DequeueBlocks removes epoch blocks from the store like dequeuing a queue,
	// which is deleting data from the oldest epoch to some new epoch
	DequeueBlocks(epochUntil uint64) error
	// DequeueTransactions removes epoch transactions from the store like dequeuing a queue,
	// which is deleting data from the oldest epoch to some new epoch
	DequeueTransactions(epochUntil uint64) error
	// DequeueLogs removes epoch logs from the store like dequeuing a queue,
	// which is deleting data from the oldest epoch to some new epoch
	DequeueLogs(epochUntil uint64) error
}

// Readable is used for RPC to read cached data from database.
type Readable interface {
	GetLogs(filter LogFilter) ([]Log, error)

	GetTransaction(txHash types.Hash) (*Transaction, error)
	GetReceipt(txHash types.Hash) (*TransactionReceipt, error)

	GetBlocksByEpoch(epochNumber uint64) ([]types.Hash, error)
	GetBlockByEpoch(epochNumber uint64) (*Block, error)
	GetBlockSummaryByEpoch(epochNumber uint64) (*BlockSummary, error)
	GetBlockByHash(blockHash types.Hash) (*Block, error)
	GetBlockSummaryByHash(blockHash types.Hash) (*BlockSummary, error)
	GetBlockByBlockNumber(blockNumber uint64) (*Block, error)
	GetBlockSummaryByBlockNumber(blockNumber uint64) (*BlockSummary, error)
}

// Store is implemented by any object that persist blockchain data, especially for event logs.
type Store interface {
	Readable
	Prunable
	io.Closer

	IsRecordNotFound(err error) bool

	GetGlobalEpochRange() (uint64, uint64, error)

	// Push appends epoch data to the store
	Push(data *EpochData) error
	Pushn(dataSlice []*EpochData) error
	// Pop removes epoch data from the store like popping a stack, which is deleting
	// data from the most recently appended epoch to some old epoch
	Pop() error
	Popn(epochUntil uint64) error

	// LoadConfig load configurations with specified names
	LoadConfig(confNames ...string) (map[string]interface{}, error)
	// StoreConfig stores configuration name to value pair
	StoreConfig(confName string, confVal interface{}) error
}

type CacheStore interface {
	Store

	// Flush deletes all kv pairs in cache
	Flush() error
}

func StoreConfig() *storeConfig {
	return &cfxStoreConfig
}

func EthStoreConfig() *storeConfig {
	return &ethStoreConfig
}

type StoreDisabler interface {
	IsChainBlockDisabled() bool
	IsChainTxnDisabled() bool
	IsChainReceiptDisabled() bool
	IsChainLogDisabled() bool
	IsDisabledForType(edt EpochDataType) bool
}

type storeConfig struct {
	// disabled store chain data types, available options are:
	// `block`, `transaction`, `receipt` and `log`
	Disables []string `default:"[block,transaction,receipt]"`

	disabledDataTypeMapping map[string]bool
}

func (conf *storeConfig) mustInit(viperRoot string) {
	viper.MustUnmarshalKey(viperRoot, conf)

	dataTypeMapping := make(map[string]bool, 4)
	for _, dt := range []string{"block", "transaction", "receipt", "log"} {
		dataTypeMapping[dt] = false
	}

	for _, dt := range conf.Disables {
		ldt := strings.ToLower(dt)

		if _, ok := dataTypeMapping[ldt]; !ok {
			logrus.WithField("dataType", dt).Fatal(
				"Failed to init store config due to invalid disabled store data type",
			)
		}

		dataTypeMapping[ldt] = true
	}

	conf.disabledDataTypeMapping = dataTypeMapping
}

func (conf *storeConfig) IsChainBlockDisabled() bool {
	return conf.disabledDataTypeMapping["block"]
}

func (conf *storeConfig) IsChainTxnDisabled() bool {
	return conf.disabledDataTypeMapping["transaction"]
}

func (conf *storeConfig) IsChainReceiptDisabled() bool {
	return conf.disabledDataTypeMapping["receipt"]
}

func (conf *storeConfig) IsChainLogDisabled() bool {
	return conf.disabledDataTypeMapping["log"]
}

func (conf *storeConfig) IsDisabledForType(edt EpochDataType) bool {
	switch edt {
	case EpochBlock:
		return conf.IsChainBlockDisabled()
	case EpochTransaction:
		return conf.IsChainTxnDisabled() && conf.IsChainReceiptDisabled()
	case EpochLog:
		return conf.IsChainLogDisabled()
	}

	return false
}

func init() {
	cfxStoreConfig.mustInit("store")
	ethStoreConfig.mustInit("ethstore")
}
