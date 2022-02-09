package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/store"
	"github.com/ethereum/go-ethereum/common"
	web3Types "github.com/openweb3/web3go/types"
)

// ethHandler interface delegated to handle eth rpc request
type ethHandler interface {
	GetBlockByHash(ctx context.Context, blockHash common.Hash, includeTxs bool) (*web3Types.Block, error)
	GetBlockByNumber(ctx context.Context, blockNum *web3Types.BlockNumber, includeTxs bool) (*web3Types.Block, error)
	GetLogs(ctx context.Context, filter store.LogFilter) ([]web3Types.Log, error)
	GetTransactionByHash(ctx context.Context, hash common.Hash) (*web3Types.Transaction, error)
	GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*web3Types.Receipt, error)
}

// EthStoreHandler implements ethHandler interface to accelerate rpc request handling by
// loading ETH blockchain data from store
type EthStoreHandler struct {
	store store.Store
	next  ethHandler
}

func NewEthStoreHandler(store store.Store, next ethHandler) *EthStoreHandler {
	return &EthStoreHandler{store: store, next: next}
}

// TODO: implement ethHandler interface by bridging Conflux to ETH.

func (h *EthStoreHandler) GetBlockByHash(ctx context.Context, blockHash common.Hash, includeTxs bool) (*web3Types.Block, error) {
	// TODO
	return nil, nil
}

func (h *EthStoreHandler) GetBlockByNumber(ctx context.Context, blockNum *web3Types.BlockNumber, includeTxs bool) (*web3Types.Block, error) {
	// TODO
	return nil, nil
}

func (h *EthStoreHandler) GetLogs(ctx context.Context, filter store.LogFilter) ([]web3Types.Log, error) {
	// TODO
	return nil, nil
}

func (h *EthStoreHandler) GetTransactionByHash(ctx context.Context, hash common.Hash) (*web3Types.Transaction, error) {
	// TODO
	return nil, nil
}

func (h *EthStoreHandler) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*web3Types.Receipt, error) {
	// TODO
	return nil, nil
}
