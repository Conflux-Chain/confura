package rpc

import (
	"context"
	"math/big"

	cacheTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	vfclient "github.com/Conflux-Chain/confura/virtualfilter/client"
	"github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	rpcMethodEthGetLogs = "eth_getLogs"

	// The maximum number of percentile values to sample from each block's
	// effective priority fees per gas in ascending order.
	maxRewardPercentileCnt = 50
	// The maximum number of blocks in the requested range for fee history.
	maxFeeHistoryBlockCnt = 1024
)

var (
	ethEmptyLogs = []web3Types.Log{}

	errTooManyRewardPercentiles = errors.Errorf(
		"the number of reward percentiles exceeds the maximum allowed (%v)",
		maxRewardPercentileCnt,
	)
	errTooManyBlocksForFeeHistory = errors.Errorf(
		"the number of blocks in the requested range exceeds the maximum allowed (%v)",
		maxFeeHistoryBlockCnt,
	)

	errNoMatchingReceiptFound = errors.New("no matching receipts found: this may indicate potential data corruption")
)

type EthAPIOption struct {
	StoreHandler        *handler.EthStoreHandler
	LogApiHandler       *handler.EthLogsApiHandler
	TxnHandler          *handler.EthTxnHandler
	VirtualFilterClient *vfclient.EthClient
}

// ethAPI provides Ethereum relative API within evm space according to:
// https://github.com/Pana/conflux-doc/blob/master/docs/evm_space_zh.md
type ethAPI struct {
	EthAPIOption

	provider         *node.EthClientProvider
	inputBlockMetric metrics.InputBlockMetric
	stateHandler     *handler.EthStateHandler

	// return empty data before eSpace hardfork block number
	hardforkBlockNumber web3Types.BlockNumber
}

func mustNewEthAPI(provider *node.EthClientProvider, option ...EthAPIOption) *ethAPI {
	client, err := provider.GetClientRandom()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get eth client randomly")
	}

	chainId, err := client.Eth.ChainId()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to get eth chain id")
	}

	if chainId == nil {
		logrus.Fatal("chain id on eSpace is nil")
	}

	var opt EthAPIOption
	if len(option) > 0 {
		opt = option[0]
	}

	return &ethAPI{
		EthAPIOption:        opt,
		provider:            provider,
		stateHandler:        handler.NewEthStateHandler(provider),
		hardforkBlockNumber: util.GetEthHardforkBlockNumber(*chainId),
	}
}

// GetBlockByHash returns the requested block. When fullTx is true all transactions in
// the block are returned in full detail, otherwise only the transaction hash is returned.
func (api *ethAPI) GetBlockByHash(ctx context.Context, blockHash common.Hash, fullTx bool) (any, error) {
	metrics.Registry.RPC.Percentage("eth_getBlockByHash", "fullTx").Mark(fullTx)

	if !store.EthStoreConfig().IsChainBlockDisabled() && !util.IsInterfaceValNil(api.StoreHandler) {
		block, err := api.StoreHandler.GetBlockByHash(ctx, blockHash, fullTx)
		metrics.Registry.RPC.StoreHit("eth_getBlockByHash", "store").Mark(err == nil)
		if err == nil {
			return block, nil
		}
	}

	return GetEthClientFromContext(ctx).Eth.LazyBlockByHash(blockHash, fullTx)
}

// ChainId returns the chainID value for transaction replay protection.
func (api *ethAPI) ChainId(ctx context.Context) (*hexutil.Uint64, error) {
	w3c := GetEthClientFromContext(ctx)
	chainId, err := w3c.Eth.ChainId()
	return (*hexutil.Uint64)(chainId), err
}

// BlockNumber returns the block number of the chain head.
func (api *ethAPI) BlockNumber(ctx context.Context) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	blockNum, err := w3c.Eth.BlockNumber()
	return (*hexutil.Big)(blockNum), err
}

// GetBalance returns the amount of wei for the given address in the state of the
// given block number.
func (api *ethAPI) GetBalance(
	ctx context.Context, address common.Address, blockNumOrHash *web3Types.BlockNumberOrHash,
) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getBalance", w3c.Eth)
	balance, err := api.stateHandler.Balance(ctx, w3c, address, blockNumOrHash)
	return (*hexutil.Big)(balance), err
}

// GetBlockByNumber returns the requested canonical block.
//   - When blockNr is -1 the chain head is returned.
//   - When blockNr is -2 the pending chain head is returned.
//   - When fullTx is true all transactions in the block are returned, otherwise
//     only the transaction hash is returned.
func (api *ethAPI) GetBlockByNumber(
	ctx context.Context, blockNum web3Types.BlockNumber, fullTx bool,
) (any, error) {
	w3c := GetEthClientFromContext(ctx)
	metrics.Registry.RPC.Percentage("eth_getBlockByNumber", "fullTx").Mark(fullTx)
	api.inputBlockMetric.Update1(&blockNum, "eth_getBlockByNumber", w3c.Eth)

	if !store.EthStoreConfig().IsChainBlockDisabled() && !util.IsInterfaceValNil(api.StoreHandler) {
		block, err := api.StoreHandler.GetBlockByNumber(ctx, &blockNum, fullTx)
		metrics.Registry.RPC.StoreHit("eth_getBlockByNumber", "store").Mark(err == nil)
		if err == nil {
			return block, nil
		}
	}

	return w3c.Eth.LazyBlockByNumber(blockNum, fullTx)
}

// GetUncleByBlockNumberAndIndex returns the uncle block for the given block hash and index.
func (api *ethAPI) GetUncleByBlockNumberAndIndex(
	ctx context.Context, blockNr web3Types.BlockNumber, index hexutil.Uint,
) (*web3Types.Block, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update1(&blockNr, "eth_getUncleByBlockNumberAndIndex", w3c.Eth)
	return w3c.Eth.UncleByBlockNumberAndIndex(blockNr, uint(index))
}

// GetUncleCountByBlockHash returns the number of uncles in a block from a block matching
// the given block hash.
func (api *ethAPI) GetUncleCountByBlockHash(ctx context.Context, hash common.Hash) (
	*hexutil.Big, error,
) {
	w3c := GetEthClientFromContext(ctx)
	count, err := w3c.Eth.BlockUnclesCountByHash(hash)
	return (*hexutil.Big)(count), err
}

// GetUncleCountByBlockNumber returns the number of uncles in a block from a block matching
// the given block number.
func (api *ethAPI) GetUncleCountByBlockNumber(ctx context.Context, blockNum web3Types.BlockNumber) (
	*hexutil.Big, error,
) {
	w3c := GetEthClientFromContext(ctx)
	count, err := w3c.Eth.BlockUnclesCountByNumber(blockNum)
	return (*hexutil.Big)(count), err
}

// ProtocolVersion returns the current ethereum protocol version.
func (api *ethAPI) ProtocolVersion(ctx context.Context) (string, error) {
	return GetEthClientFromContext(ctx).Eth.ProtocolVersion()
}

// GasPrice returns the current gas price in wei.
func (api *ethAPI) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	gasPrice, err := w3c.Eth.GasPrice()
	return (*hexutil.Big)(gasPrice), err
}

// GetStorageAt returns the value from a storage position at a given address.
func (api *ethAPI) GetStorageAt(
	ctx context.Context, address common.Address, location *hexutil.Big, blockNumOrHash *web3Types.BlockNumberOrHash,
) (common.Hash, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getStorageAt", w3c.Eth)
	return api.stateHandler.StorageAt(ctx, w3c, address, (*big.Int)(location), blockNumOrHash)
}

// GetCode returns the contract code of the given account.
// The block number can be nil, in which case the code is taken from the latest known block.
func (api *ethAPI) GetCode(
	ctx context.Context, account common.Address, blockNumOrHash *web3Types.BlockNumberOrHash,
) (hexutil.Bytes, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getCode", w3c.Eth)
	return api.stateHandler.CodeAt(ctx, w3c, account, blockNumOrHash)
}

// GetTransactionCount returns the number of transactions (nonce) sent from the given account.
// The block number can be nil, in which case the nonce is taken from the latest known block.
func (api *ethAPI) GetTransactionCount(
	ctx context.Context, account common.Address, blockNumOrHash *web3Types.BlockNumberOrHash,
) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getTransactionCount", w3c.Eth)
	count, err := api.stateHandler.TransactionCount(ctx, w3c, account, blockNumOrHash)
	return (*hexutil.Big)(count), err
}

// SendRawTransaction injects a signed transaction into the pending pool for execution.
//
// If the transaction was a contract creation use the TransactionReceipt method to get the
// contract address after the transaction has been mined.
func (api *ethAPI) SendRawTransaction(ctx context.Context, signedTx hexutil.Bytes) (common.Hash, error) {
	w3c := GetEthClientFromContext(ctx)

	if api.TxnHandler != nil {
		cgroup := GetClientGroupFromContext(ctx)
		return api.TxnHandler.SendRawTxn(w3c, cgroup, signedTx)
	}

	return w3c.Eth.SendRawTransaction(signedTx)
}

// SubmitTransaction is an alias of `SendRawTransaction` method.
func (api *ethAPI) SubmitTransaction(ctx context.Context, signedTx hexutil.Bytes) (common.Hash, error) {
	return api.SendRawTransaction(ctx, signedTx)
}

// Call executes a new message call immediately without creating a transaction on the block chain.
func (api *ethAPI) Call(
	ctx context.Context, request web3Types.CallRequest, blockNumOrHash *web3Types.BlockNumberOrHash,
) (hexutil.Bytes, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update2(blockNumOrHash, "eth_call", w3c.Eth)
	return api.stateHandler.Call(ctx, w3c, request, blockNumOrHash)
}

// EstimateGas generates and returns an estimate of how much gas is necessary to allow the transaction
// to complete. The transaction will not be added to the blockchain. Note that the estimate may be
// significantly more than the amount of gas actually used by the transaction, for a variety of reasons
// including EVM mechanics and node performance or miner policy.
func (api *ethAPI) EstimateGas(
	ctx context.Context, request web3Types.CallRequest, blockNumOrHash *web3Types.BlockNumberOrHash,
) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update2(blockNumOrHash, "eth_estimateGas", w3c.Eth)
	gas, err := api.stateHandler.EstimateGas(ctx, w3c, request, blockNumOrHash)
	return (*hexutil.Big)(gas), err
}

// GetTransactionByHash returns the transaction with the given hash.
func (api *ethAPI) GetTransactionByHash(ctx context.Context, hash common.Hash) (any, error) {
	if !store.EthStoreConfig().IsChainTxnDisabled() && !util.IsInterfaceValNil(api.StoreHandler) {
		tx, err := api.StoreHandler.GetTransactionByHash(ctx, hash)
		metrics.Registry.RPC.StoreHit("eth_getTransactionByHash", "store").Mark(err == nil)
		if err == nil {
			return tx, nil
		}
	}

	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.LazyTransactionByHash(hash)
}

// GetTransactionReceipt returns the receipt of a transaction by transaction hash.
// Note that the receipt is not available for pending transactions.
func (api *ethAPI) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (_ any, err error) {
	var found bool
	defer func() {
		if err == nil {
			metrics.Registry.RPC.Percentage("eth_getTransactionReceipt", "notfound").Mark(!found)
		}
	}()

	if !store.EthStoreConfig().IsChainReceiptDisabled() && !util.IsInterfaceValNil(api.StoreHandler) {
		var receipt *web3Types.Receipt
		receipt, err = api.StoreHandler.GetTransactionReceipt(ctx, txHash)
		metrics.Registry.RPC.StoreHit("eth_getTransactionReceipt", "store").Mark(err == nil)
		if err == nil {
			found = (receipt != nil)
			return receipt, nil
		}
	}

	w3c := GetEthClientFromContext(ctx)
	lazyReceipt, err := w3c.Eth.LazyTransactionReceipt(txHash)
	if err != nil {
		return nil, err
	}
	if !viper.GetBool("ethrpc.reValidation") {
		found = !lazyReceipt.IsEmptyOrNull()
		return lazyReceipt, nil
	}

	receipt, err := lazyReceipt.Load()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to load lazy receipt")
	}

	if receipt != nil && receipt.TransactionHash == txHash {
		found = true
		return receipt, nil
	}

	txn, err := w3c.Eth.TransactionByHash(txHash)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to retrieve transaction for data correctness validation")
	}
	if txn == nil || txn.BlockHash == nil {
		// Transaction not found or not mined
		return nil, nil
	}

	// If the initial receipt's block hash matches the transaction's block hash, it's considered valid
	if receipt != nil && receipt.BlockHash == *txn.BlockHash {
		found = true
		return receipt, nil
	}

	// Attempt to correlate with other clients if the initial receipt doesn't match.
	clients, err := GetEthClientProviderFromContext(ctx).GetClientsByGroup(node.GroupEthHttp)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to retrieve node clients for data correctness validation")
	}
	for _, client := range clients {
		if client.URL == w3c.URL {
			// Skip the original client
			continue
		}
		var err error
		receipt, err = client.Eth.TransactionReceipt(txHash)
		if err != nil {
			continue
		}
		if receipt != nil && receipt.BlockHash == *txn.BlockHash {
			found = true
			return receipt, nil
		}
	}

	// No matching receipt found after checking other clients
	err = errNoMatchingReceiptFound
	return nil, err
}

// GetBlockReceipts returns the receipts of a given block number or hash.
func (api *ethAPI) GetBlockReceipts(ctx context.Context, blockNrOrHash web3Types.BlockNumberOrHash) (any, error) {
	w3c := GetEthClientFromContext(ctx)
	lazyReceipts, err := w3c.Eth.LazyBlockReceipts(&blockNrOrHash)
	if err != nil {
		return nil, err
	}
	if !viper.GetBool("ethrpc.reValidation") {
		return lazyReceipts, nil
	}

	receipts, err := lazyReceipts.Load()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to load lazy receipts")
	}

	var block *web3Types.Block
	if blockNum, ok := blockNrOrHash.Number(); ok {
		block, err = w3c.Eth.BlockByNumber(blockNum, false)
	} else if blockHash, ok := blockNrOrHash.Hash(); ok {
		block, err = w3c.Eth.BlockByHash(blockHash, false)
	}
	if err != nil {
		return nil, errors.WithMessage(err, "failed to retrieve block for data correctness validation")
	}
	if block == nil { // Block not found
		return nil, nil
	}
	if len(block.Transactions.Hashes()) == 0 { // No transactions included in block
		return []*web3Types.Receipt{}, nil
	}

	numTxns := len(block.Transactions.Hashes())
	if len(receipts) == numTxns && receipts[0].BlockHash == block.Hash {
		return receipts, nil
	}

	// Attempt to correlate with other clients if the initial receipt doesn't match.
	clients, err := GetEthClientProviderFromContext(ctx).GetClientsByGroup(node.GroupEthHttp)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to retrieve node clients for data correctness validation")
	}
	for _, client := range clients {
		if client.URL == w3c.URL {
			// Skip the original client
			continue
		}
		receipts, err := client.Eth.BlockReceipts(&blockNrOrHash)
		if err != nil {
			continue
		}
		if len(receipts) == numTxns && receipts[0].BlockHash == block.Hash {
			return receipts, nil
		}
	}

	// No matching receipt found after checking other clients
	return nil, errNoMatchingReceiptFound
}

// Returns pending transactions for a given account.
func (api *ethAPI) GetAccountPendingTransactions(
	ctx context.Context, addr common.Address, startNonce *hexutil.Big, limit *hexutil.Uint64,
) (*web3Types.AccountPendingTransactions, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.AccountPendingTransactions(addr, startNonce.ToInt(), (*uint64)(limit))
}

// GetLogs returns an array of all logs matching a given filter object.
func (api *ethAPI) GetLogs(ctx context.Context, fq web3Types.FilterQuery) ([]web3Types.Log, error) {
	w3c := GetEthClientFromContext(ctx)
	return api.getLogs(ctx, w3c, &fq, rpcMethodEthGetLogs)
}

// getLogs helper method to get logs from store or fullnode.
func (api *ethAPI) getLogs(
	ctx context.Context,
	w3c *node.Web3goClient,
	fq *web3Types.FilterQuery,
	rpcMethod string,
) ([]web3Types.Log, error) {
	metrics.UpdateEthRpcLogFilter(rpcMethod, w3c.Eth, fq)

	flag, ok := ParseEthLogFilterType(fq)
	if !ok {
		return ethEmptyLogs, ErrInvalidEthLogFilter
	}

	if err := NormalizeEthLogFilter(w3c, flag, fq, api.hardforkBlockNumber); err != nil {
		return ethEmptyLogs, err
	}

	if err := ValidateEthLogFilter(flag, fq); err != nil {
		return ethEmptyLogs, err
	}

	// return empty directly if filter block range before eSpace hardfork
	if fq.ToBlock != nil && *fq.ToBlock <= api.hardforkBlockNumber {
		return ethEmptyLogs, nil
	}

	if api.LogApiHandler != nil {
		logs, hitStore, err := api.LogApiHandler.GetLogs(ctx, w3c.Client.Eth, fq, rpcMethod)
		metrics.Registry.RPC.StoreHit(rpcMethod, "store").Mark(hitStore)
		return uniformEthLogs(logs), err
	}

	// fail over to fullnode if no handler configured
	return w3c.Eth.Logs(*fq)
}

// GetBlockTransactionCountByHash returns the total number of transactions in the given block.
func (api *ethAPI) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	count, err := w3c.Eth.BlockTransactionCountByHash(blockHash)
	return (*hexutil.Big)(count), err
}

// GetBlockTransactionCountByNumber returns the number of transactions in the block with the given
// block number.
func (api *ethAPI) GetBlockTransactionCountByNumber(ctx context.Context, blockNum web3Types.BlockNumber) (
	*hexutil.Big, error,
) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update1(&blockNum, "eth_getBlockTransactionCountByNumber", w3c.Eth)
	count, err := w3c.Eth.BlockTransactionCountByNumber(blockNum)
	return (*hexutil.Big)(count), err
}

// Syncing returns an object with data about the sync status or false.
// https://openethereum.github.io/JSONRPC-eth-module#eth_syncing
func (api *ethAPI) Syncing(ctx context.Context) (web3Types.SyncStatus, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.Syncing()
}

// Hashrate returns the number of hashes per second that the node is mining with.
// Only applicable when the node is mining.
func (api *ethAPI) Hashrate(ctx context.Context) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	hashrate, err := w3c.Eth.Hashrate()
	return (*hexutil.Big)(hashrate), err
}

// Coinbase returns the client coinbase address..
func (api *ethAPI) Coinbase(ctx context.Context) (common.Address, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.Author()
}

// Mining returns true if client is actively mining new blocks.
func (api *ethAPI) Mining(ctx context.Context) (bool, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.IsMining()
}

// MaxPriorityFeePerGas returns a fee per gas that is an estimate of how much you can pay as
// a priority fee, or "tip", to get a transaction included in the current block.
func (api *ethAPI) MaxPriorityFeePerGas(ctx context.Context) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	priorityFee, err := w3c.Eth.MaxPriorityFeePerGas()
	return (*hexutil.Big)(priorityFee), err
}

// Accounts returns a list of addresses owned by client.
func (api *ethAPI) Accounts(ctx context.Context) ([]common.Address, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.Accounts()
}

// SubmitHashrate used for submitting mining hashrate.
func (api *ethAPI) SubmitHashrate(
	ctx context.Context, hashrate *hexutil.Big, clientId common.Hash,
) (bool, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.SubmitHashrate((*big.Int)(hashrate), clientId)
}

// GetUncleByBlockHashAndIndex returns information about the 'Uncle' of a block by hash and
// the Uncle index position.
func (api *ethAPI) GetUncleByBlockHashAndIndex(
	ctx context.Context, hash common.Hash, index hexutil.Uint,
) (*web3Types.Block, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.UncleByBlockHashAndIndex(hash, index)
}

// GetTransactionByBlockHashAndIndex returns information about a transaction by block hash and
// transaction index position.
func (api *ethAPI) GetTransactionByBlockHashAndIndex(
	ctx context.Context, hash common.Hash, index hexutil.Uint,
) (cacheTypes.Lazy[*web3Types.TransactionDetail], error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.LazyTransactionByBlockHashAndIndex(hash, uint(index))
}

// GetTransactionByBlockNumberAndIndex returns information about a transaction by block number and
// transaction index position.
func (api *ethAPI) GetTransactionByBlockNumberAndIndex(
	ctx context.Context, blockNum web3Types.BlockNumber, index hexutil.Uint,
) (cacheTypes.Lazy[*web3Types.TransactionDetail], error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update1(&blockNum, "eth_getTransactionByBlockNumberAndIndex", w3c.Eth)
	return w3c.Eth.LazyTransactionByBlockNumberAndIndex(blockNum, uint(index))
}

// FeeHistory returns historical gas information, which could be used for tracking trends over time.
func (api *ethAPI) FeeHistory(
	ctx context.Context, blockCount types.HexOrDecimalUint64, lastBlock web3Types.BlockNumber, rewardPercentiles []float64,
) (val *web3Types.FeeHistory, err error) {
	if blockCount > maxFeeHistoryBlockCnt {
		return nil, errTooManyBlocksForFeeHistory
	}

	if len(rewardPercentiles) > maxRewardPercentileCnt {
		return nil, errTooManyRewardPercentiles
	}

	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.FeeHistory(uint64(blockCount), lastBlock, rewardPercentiles)
}
