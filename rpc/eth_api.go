package rpc

import (
	"context"

	"github.com/conflux-chain/conflux-infura/node"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

// ethAPI provides ethereum relative API within EVM space according to:
// https://github.com/Pana/conflux-doc/blob/master/docs/evm_space_zh.md
type ethAPI struct {
	provider *node.ClientProvider
}

func newEthAPI(provider *node.ClientProvider) *ethAPI {
	return &ethAPI{provider: provider}
}

// GetBlockByHash returns the requested block. When fullTx is true all transactions in
// the block are returned in full detail, otherwise only the transaction hash is returned.
func (api *ethAPI) GetBlockByHash(
	ctx context.Context, hash common.Hash, fullTx bool,
) (interface{}, error) {
	// TODO: add implementation
	return nil, nil
}

// ChainId returns the chainID value for transaction replay protection.
func (api *ethAPI) ChainId(ctx context.Context) (*hexutil.Big, error) {
	// TODO: add implementation
	return nil, nil
}

// BlockNumber returns the block number of the chain head.
func (api *ethAPI) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	// TODO: add implementation
	return 0, nil
}

// GetBalance returns the amount of wei for the given address in the state of the
// given block number.
func (api *ethAPI) GetBalance(
	ctx context.Context, address common.Address, blockNum *rpc.BlockNumber,
) (*hexutil.Big, error) {
	// TODO: add implementation
	return nil, nil
}

// GetBlockByNumber returns the requested canonical block.
// * When blockNr is -1 the chain head is returned.
// * When blockNr is -2 the pending chain head is returned.
// * When fullTx is true all transactions in the block are returned, otherwise
//   only the transaction hash is returned.
func (api *ethAPI) GetBlockByNumber(
	ctx context.Context, blockNum *rpc.BlockNumber, fullTx bool,
) (interface{}, error) {
	// TODO: add implementation
	return nil, nil
}

// GetUncleByBlockNumberAndIndex returns the uncle block for the given block hash and index.
func (api *ethAPI) GetUncleByBlockNumberAndIndex(
	ctx context.Context, blockNr *rpc.BlockNumber, index hexutil.Uint,
) (*types.Header, error) {
	// TODO: add implementation
	return nil, nil
}

// GetUncleCountByBlockHash returns the number of uncles in a block from a block matching
// the given block hash.
func (api *ethAPI) GetUncleCountByBlockHash(ctx context.Context, hash common.Hash) (
	hexutil.Uint64, error,
) {
	// TODO: add implementation
	return 0, nil
}

// ProtocolVersion returns the current ethereum protocol version.
func (api *ethAPI) ProtocolVersion(ctx context.Context) (string, error) {
	// TODO: add implementation
	return "", nil
}

// GasPrice returns the current gas price in wei.
func (api *ethAPI) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	return nil, nil
}

// GetStorageAt returns the value from a storage position at a given address.
func (api *ethAPI) GetStorageAt(
	ctx context.Context, address common.Address, key common.Hash, blockNum *rpc.BlockNumber,
) (hexutil.Bytes, error) {
	// TODO: add implementation
	return nil, nil
}

// GetCode returns the contract code of the given account.
// The block number can be nil, in which case the code is taken from the latest known block.
func (api *ethAPI) GetCode(
	ctx context.Context, account common.Address, blockNum *rpc.BlockNumber,
) (hexutil.Bytes, error) {
	// TODO: add implementation
	return nil, nil
}

// GetTransactionCount returns the number of transactions (nonce) sent from the given account.
// The block number can be nil, in which case the nonce is taken from the latest known block.
func (api *ethAPI) GetTransactionCount(
	ctx context.Context, account common.Address, blockNum *rpc.BlockNumber,
) (hexutil.Uint64, error) {
	// TODO: add implementation
	return 0, nil
}

// SendTransaction injects a signed transaction into the pending pool for execution.
//
// If the transaction was a contract creation use the TransactionReceipt method to get the
// contract address after the transaction has been mined.
func (api *ethAPI) SendRawTransaction(ctx context.Context, signedTx hexutil.Bytes) (common.Hash, error) {
	// TODO: add implementation
	return common.Hash{}, nil
}

// SubmitTransaction is an alias of `SendRawTransaction` method.
func (api *ethAPI) SubmitTransaction(ctx context.Context, signedTx hexutil.Bytes) (common.Hash, error) {
	return api.SendRawTransaction(ctx, signedTx)
}

// Call executes a new message call immediately without creating a transaction on the block chain.
func (api *ethAPI) Call(
	ctx context.Context, msg ethereum.CallMsg, blockNum *rpc.BlockNumber,
) (hexutil.Bytes, error) {
	// TODO: add implementation
	return nil, nil
}

// EstimateGas generates and returns an estimate of how much gas is necessary to allow the transaction
// to complete. The transaction will not be added to the blockchain. Note that the estimate may be
// significantly more than the amount of gas actually used by the transaction, for a variety of reasons
// including EVM mechanics and node performance or miner policy.
func (api *ethAPI) EstimateGas(ctx context.Context, msg ethereum.CallMsg) (*hexutil.Big, error) {
	// TODO: add implementation
	return nil, nil
}

// TransactionByHash returns the transaction with the given hash.
func (api *ethAPI) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, error) {
	// TODO: add implementation
	return nil, nil
}

// TransactionReceipt returns the receipt of a transaction by transaction hash.
// Note that the receipt is not available for pending transactions.
func (api *ethAPI) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	// TODO: add implementation
	return nil, nil
}

// GetLogs returns an array of all logs matching a given filter object.
func (api *ethAPI) GetLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	// TODO: add implementation
	return nil, nil
}

// GetBlockTransactionCountByHash returns the total number of transactions in the given block.
func (api *ethAPI) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (
	hexutil.Uint64, error,
) {
	// TODO: add implementation
	return 0, nil
}

// GetBlockTransactionCountByNumber returns the number of transactions in the block with the given
// block number.
func (api *ethAPI) GetBlockTransactionCountByNumber(ctx context.Context, blockNum *rpc.BlockNumber) (
	hexutil.Uint64, error,
) {
	// TODO: add implementation
	return 0, nil
}

// Syncing returns an object with data about the sync status or false.
// https://openethereum.github.io/JSONRPC-eth-module#eth_syncing
func (api *ethAPI) Syncing(ctx context.Context) (interface{}, error) {
	// TODO: add implementation
	return (*ethereum.SyncProgress)(nil), nil
}

// Hashrate returns the number of hashes per second that the node is mining with.
// Only applicable when the node is mining.
func (api *ethAPI) Hashrate(ctx context.Context) (hexutil.Uint64, error) {
	// TODO: add implementation
	return 0, nil
}

// Coinbase returns the client coinbase address..
func (api *ethAPI) Coinbase(ctx context.Context) (common.Address, error) {
	// TODO: add implementation
	return common.Address{}, nil
}

// Mining returns true if client is actively mining new blocks.
func (api *ethAPI) Mining(ctx context.Context) (bool, error) {
	// TODO: add implementation
	return false, nil
}

// MaxPriorityFeePerGas returns a fee per gas that is an estimate of how much you can pay as
// a priority fee, or "tip", to get a transaction included in the current block.
func (api *ethAPI) MaxPriorityFeePerGas(ctx context.Context) (*hexutil.Big, error) {
	// TODO: add implementation
	return nil, nil
}

// Accounts returns a list of addresses owned by client.
func (api *ethAPI) Accounts(ctx context.Context) ([]common.Address, error) {
	// TODO: add implementation
	return []common.Address{}, nil
}

// SubmitHashrate used for submitting mining hashrate.
func (api *ethAPI) SubmitHashrate(
	ctx context.Context, hashrate hexutil.Uint64, clientId common.Hash,
) (bool, error) {
	// TODO: add implementation
	return false, nil
}

// GetUncleByBlockHashAndIndex returns information about the 'Uncle' of a block by hash and
// the Uncle index position.
func (api *ethAPI) GetUncleByBlockHashAndIndex(
	ctx context.Context, hash common.Hash, index hexutil.Uint64,
) (*types.Block, error) {
	// TODO: add implementation
	return nil, nil
}

// The following RPC methods are not supported yet by the fullnode:
// `eth_getTransactionByBlockHashAndIndex`
// `eth_getTransactionByBlockNumberAndIndex`
// `eth_feeHistory`
// `eth_getFilterChanges`
// `eth_getFilterLogs`
// `eth_newBlockFilter`
// `eth_newFilter`
// `eth_newPendingTransactionFilter`
// `eth_uninstallFilter`

// GetTransactionByBlockHashAndIndex returns information about a transaction by block hash and
// transaction index position.
func (api *ethAPI) GetTransactionByBlockHashAndIndex(
	ctx context.Context, hash common.Hash, index hexutil.Uint64,
) (*types.Transaction, error) {
	// TODO: implement it until fullnode support
	return nil, errRpcNotSupported
}

// GetTransactionByBlockNumberAndIndex returns information about a transaction by block number and
// transaction index position.
func (api *ethAPI) GetTransactionByBlockNumberAndIndex(
	ctx context.Context, blockNum rpc.BlockNumber, index hexutil.Uint64,
) (*types.Transaction, error) {
	// TODO: implement it until fullnode support
	return nil, errRpcNotSupported
}
