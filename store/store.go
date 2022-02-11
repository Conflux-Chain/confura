package store

import (
	"io"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

// Store is implemented by any object that persist blockchain data, especially for event logs.
type Store interface {
	io.Closer

	IsRecordNotFound(err error) bool

	GetBlockEpochRange() (uint64, uint64, error)
	GetTransactionEpochRange() (uint64, uint64, error)
	GetLogEpochRange() (uint64, uint64, error)
	GetGlobalEpochRange() (uint64, uint64, error)

	GetNumBlocks() uint64
	GetNumTransactions() uint64
	GetNumLogs() uint64

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

	// Push appends epoch data to the store
	Push(data *EpochData) error
	Pushn(dataSlice []*EpochData) error
	// Pop removes epoch data from the store like popping a stack, which is deleting
	// data from the most recently appended epoch to some old epoch
	Pop() error
	Popn(epochUntil uint64) error

	// DequeueBlocks removes epoch blocks from the store like dequeuing a queue,
	// which is deleting data from the oldest epoch to some new epoch
	DequeueBlocks(epochUntil uint64) error
	// DequeueTransactions removes epoch transactions from the store like dequeuing a queue,
	// which is deleting data from the oldest epoch to some new epoch
	DequeueTransactions(epochUntil uint64) error
	// DequeueLogs removes epoch logs from the store like dequeuing a queue,
	// which is deleting data from the oldest epoch to some new epoch
	DequeueLogs(epochUntil uint64) error

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
