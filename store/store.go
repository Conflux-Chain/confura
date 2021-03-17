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

	GetLogs(filter LogFilter) ([]types.Log, error)

	GetTransaction(txHash types.Hash) (*types.Transaction, error)
	GetReceipt(txHash types.Hash) (*types.TransactionReceipt, error)

	GetBlocksByEpoch(epochNumber uint64) ([]types.Hash, error)
	GetBlockByEpoch(epochNumber uint64) (*types.Block, error)
	GetBlockSummaryByEpoch(epochNumber uint64) (*types.BlockSummary, error)
	GetBlockByHash(blockHash types.Hash) (*types.Block, error)
	GetBlockSummaryByHash(blockHash types.Hash) (*types.BlockSummary, error)

	Push(data *EpochData) error
	Pushn(dataSlice []*EpochData) error
	Pop() error
	Popn(epochUntil uint64) error
}
