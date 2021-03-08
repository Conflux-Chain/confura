package store

import (
	"math/big"

	"github.com/Conflux-Chain/go-conflux-sdk/types"
)

// Store is implemented by any object that persist blockchain data, especially for event logs.
type Store interface {
	GetBlockEpochRange() (*big.Int, *big.Int, error)

	// GetLogs(filter types.LogFilter) ([]types.Log, error)

	GetTransaction(txHash types.Hash) (*types.Transaction, error)
	GetReceipt(txHash types.Hash) (*types.TransactionReceipt, error)

	GetBlocksByEpoch(epochNumber *big.Int) ([]types.Hash, error)
	GetBlockByEpoch(epochNumber *big.Int) (*types.Block, error)
	GetBlockSummaryByEpoch(epochNumber *big.Int) (*types.BlockSummary, error)
	GetBlockByHash(blockHash types.Hash) (*types.Block, error)
	GetBlockSummaryByHash(blockHash types.Hash) (*types.BlockSummary, error)

	PutEpochData(data *EpochData) error
	PutEpochDataSlice(dataSlice []*EpochData) error
	// Remove(epochFrom, epochTo *big.Int, includeLogs bool) error

	Close() error
}
