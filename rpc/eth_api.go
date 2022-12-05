package rpc

import (
	"context"
	"math/big"

	"github.com/Conflux-Chain/confura/node"
	"github.com/Conflux-Chain/confura/rpc/cache"
	"github.com/Conflux-Chain/confura/rpc/handler"
	"github.com/Conflux-Chain/confura/store"
	"github.com/Conflux-Chain/confura/util"
	"github.com/Conflux-Chain/confura/util/metrics"
	vfclient "github.com/Conflux-Chain/confura/virtualfilter/client"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

var (
	ethEmptyLogs = []web3Types.Log{}
)

type EthAPIOption struct {
	StoreHandler        *handler.EthStoreHandler
	LogApiHandler       *handler.EthLogsApiHandler
	VirtualFilterClient *vfclient.Client
}

// ethAPI provides Ethereum relative API within evm space according to:
// https://github.com/Pana/conflux-doc/blob/master/docs/evm_space_zh.md
type ethAPI struct {
	EthAPIOption

	provider         *node.EthClientProvider
	inputBlockMetric metrics.InputBlockMetric

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
		hardforkBlockNumber: util.GetEthHardforkBlockNumber(*chainId),
	}
}

// GetBlockByHash returns the requested block. When fullTx is true all transactions in
// the block are returned in full detail, otherwise only the transaction hash is returned.
func (api *ethAPI) GetBlockByHash(
	ctx context.Context, blockHash common.Hash, fullTx bool,
) (*web3Types.Block, error) {
	metrics.Registry.RPC.Percentage("eth_getBlockByHash", "fullTx").Mark(fullTx)

	logger := logrus.WithFields(logrus.Fields{
		"blockHash": blockHash.Hex(), "includeTxs": fullTx,
	})

	if !store.EthStoreConfig().IsChainBlockDisabled() && !util.IsInterfaceValNil(api.StoreHandler) {
		block, err := api.StoreHandler.GetBlockByHash(ctx, blockHash, fullTx)
		metrics.Registry.RPC.StoreHit("eth_getBlockByHash", "store").Mark(err == nil)
		if err == nil {
			logger.Debug("Loading eth data for eth_getBlockByHash hit in the store")
			return block, nil
		}

		logger.WithError(err).Debug("Loading eth data for eth_getBlockByHash missed from the ethstore")
	}

	logger.Debug("Delegating eth_getBlockByHash rpc request to fullnode")

	return GetEthClientFromContext(ctx).Eth.BlockByHash(blockHash, fullTx)
}

// ChainId returns the chainID value for transaction replay protection.
func (api *ethAPI) ChainId(ctx context.Context) (*hexutil.Uint64, error) {
	w3c := GetEthClientFromContext(ctx)
	return cache.EthDefault.GetChainId(w3c.Client)
}

// BlockNumber returns the block number of the chain head.
func (api *ethAPI) BlockNumber(ctx context.Context) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	return cache.EthDefault.GetBlockNumber(w3c)
}

// GetBalance returns the amount of wei for the given address in the state of the
// given block number.
func (api *ethAPI) GetBalance(
	ctx context.Context, address common.Address, blockNumOrHash *web3Types.BlockNumberOrHash,
) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getBalance", w3c.Eth)
	balance, err := w3c.Eth.Balance(address, blockNumOrHash)
	return (*hexutil.Big)(balance), err
}

// GetBlockByNumber returns the requested canonical block.
// * When blockNr is -1 the chain head is returned.
// * When blockNr is -2 the pending chain head is returned.
// * When fullTx is true all transactions in the block are returned, otherwise
//   only the transaction hash is returned.
func (api *ethAPI) GetBlockByNumber(
	ctx context.Context, blockNum web3Types.BlockNumber, fullTx bool,
) (*web3Types.Block, error) {
	metrics.Registry.RPC.Percentage("eth_getBlockByNumber", "fullTx").Mark(fullTx)

	logger := logrus.WithFields(logrus.Fields{
		"blockNum": blockNum, "includeTxs": fullTx,
	})

	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update1(&blockNum, "eth_getBlockByNumber", w3c.Eth)

	if !store.EthStoreConfig().IsChainBlockDisabled() && !util.IsInterfaceValNil(api.StoreHandler) {
		block, err := api.StoreHandler.GetBlockByNumber(ctx, &blockNum, fullTx)
		metrics.Registry.RPC.StoreHit("eth_getBlockByNumber", "store").Mark(err == nil)
		if err == nil {
			logger.Debug("Loading eth data for eth_getBlockByNumber hit in the store")
			return block, nil
		}

		logger.WithError(err).Debug("Loading eth data for eth_getBlockByNumber missed from the ethstore")
	}

	logger.Debug("Delegating eth_getBlockByNumber rpc request to fullnode")

	return w3c.Eth.BlockByNumber(blockNum, fullTx)
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

// ProtocolVersion returns the current ethereum protocol version.
func (api *ethAPI) ProtocolVersion(ctx context.Context) (string, error) {
	return GetEthClientFromContext(ctx).Eth.ProtocolVersion()
}

// GasPrice returns the current gas price in wei.
func (api *ethAPI) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	return cache.EthDefault.GetGasPrice(w3c.Client)
}

// GetStorageAt returns the value from a storage position at a given address.
func (api *ethAPI) GetStorageAt(
	ctx context.Context, address common.Address, location *hexutil.Big, blockNumOrHash *web3Types.BlockNumberOrHash,
) (common.Hash, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getStorageAt", w3c.Eth)
	return w3c.Eth.StorageAt(address, (*big.Int)(location), blockNumOrHash)
}

// GetCode returns the contract code of the given account.
// The block number can be nil, in which case the code is taken from the latest known block.
func (api *ethAPI) GetCode(
	ctx context.Context, account common.Address, blockNumOrHash *web3Types.BlockNumberOrHash,
) (hexutil.Bytes, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getCode", w3c.Eth)
	return w3c.Eth.CodeAt(account, blockNumOrHash)
}

// GetTransactionCount returns the number of transactions (nonce) sent from the given account.
// The block number can be nil, in which case the nonce is taken from the latest known block.
func (api *ethAPI) GetTransactionCount(
	ctx context.Context, account common.Address, blockNumOrHash *web3Types.BlockNumberOrHash,
) (*hexutil.Big, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update2(blockNumOrHash, "eth_getTransactionCount", w3c.Eth)
	count, err := w3c.Eth.TransactionCount(account, blockNumOrHash)
	return (*hexutil.Big)(count), err
}

// SendTransaction injects a signed transaction into the pending pool for execution.
//
// If the transaction was a contract creation use the TransactionReceipt method to get the
// contract address after the transaction has been mined.
func (api *ethAPI) SendRawTransaction(ctx context.Context, signedTx hexutil.Bytes) (common.Hash, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.SendRawTransaction(signedTx)
}

// SubmitTransaction is an alias of `SendRawTransaction` method.
func (api *ethAPI) SubmitTransaction(ctx context.Context, signedTx hexutil.Bytes) (common.Hash, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.SubmitTransaction(signedTx)
}

// Call executes a new message call immediately without creating a transaction on the block chain.
func (api *ethAPI) Call(
	ctx context.Context, request web3Types.CallRequest, blockNumOrHash *web3Types.BlockNumberOrHash,
) (hexutil.Bytes, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update2(blockNumOrHash, "eth_call", w3c.Eth)
	return w3c.Eth.Call(request, blockNumOrHash)
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
	gas, err := w3c.Eth.EstimateGas(request, blockNumOrHash)
	return (*hexutil.Big)(gas), err
}

// TransactionByHash returns the transaction with the given hash.
func (api *ethAPI) GetTransactionByHash(ctx context.Context, hash common.Hash) (*web3Types.TransactionDetail, error) {
	logger := logrus.WithField("txHash", hash.Hex())

	if !store.EthStoreConfig().IsChainTxnDisabled() && !util.IsInterfaceValNil(api.StoreHandler) {
		tx, err := api.StoreHandler.GetTransactionByHash(ctx, hash)
		metrics.Registry.RPC.StoreHit("eth_getTransactionByHash", "store").Mark(err == nil)
		if err == nil {
			logger.Debug("Loading eth data for eth_getTransactionByHash hit in the store")
			return tx, nil
		}

		logger.WithError(err).Debug("Loading eth data for eth_getTransactionByHash missed from the ethstore")
	}

	logger.Debug("Delegating eth_getTransactionByHash rpc request to fullnode")

	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.TransactionByHash(hash)
}

// TransactionReceipt returns the receipt of a transaction by transaction hash.
// Note that the receipt is not available for pending transactions.
func (api *ethAPI) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*web3Types.Receipt, error) {
	logger := logrus.WithField("txHash", txHash.Hex())

	if !store.EthStoreConfig().IsChainReceiptDisabled() && !util.IsInterfaceValNil(api.StoreHandler) {
		tx, err := api.StoreHandler.GetTransactionReceipt(ctx, txHash)
		metrics.Registry.RPC.StoreHit("eth_getTransactionReceipt", "store").Mark(err == nil)
		if err == nil {
			logger.Debug("Loading eth data for eth_getTransactionReceipt hit in the ethstore")
			return tx, nil
		}

		logger.WithError(err).Debug("Loading eth data for eth_getTransactionReceipt missed from the ethstore")
	}

	logger.Debug("Delegating eth_getTransactionReceipt rpc request to fullnode")

	w3c := GetEthClientFromContext(ctx)
	receipt, err := w3c.Eth.TransactionReceipt(txHash)
	if err != nil {
		metrics.Registry.RPC.Percentage("eth_getTransactionReceipt", "notfound").Mark(receipt == nil)
	}

	return receipt, err
}

// GetLogs returns an array of all logs matching a given filter object.
func (api *ethAPI) GetLogs(ctx context.Context, fq web3Types.FilterQuery) ([]web3Types.Log, error) {
	logs, delegated, err := api.getLogs(ctx, &fq)
	logger := api.filterLogger(&fq).WithField("fnDelegated", delegated)

	if err != nil {
		logger.WithError(err).Debug("Failed to handle `eth_getLogs` RPC request")
		return logs, err
	}

	logger.Debug("`eth_getLogs` RPC request handled")
	return logs, nil
}

// getLogs helper method to get logs from store or fullnode.
func (api *ethAPI) getLogs(ctx context.Context, fq *web3Types.FilterQuery) ([]web3Types.Log, bool, error) {
	w3c := GetEthClientFromContext(ctx)
	metrics.UpdateEthLogFilter("eth_getLogs", w3c.Eth, fq)

	flag, ok := ParseEthLogFilterType(fq)
	if !ok {
		return ethEmptyLogs, false, ErrInvalidEthLogFilter
	}

	if err := NormalizeEthLogFilter(w3c.Client, flag, fq, api.hardforkBlockNumber); err != nil {
		return ethEmptyLogs, false, err
	}

	if err := ValidateEthLogFilter(flag, fq); err != nil {
		return ethEmptyLogs, false, err
	}

	// return empty directly if filter block range before eSpace hardfork
	if fq.ToBlock != nil && *fq.ToBlock <= api.hardforkBlockNumber {
		return ethEmptyLogs, false, nil
	}

	if api.LogApiHandler != nil {
		logs, hitStore, err := api.LogApiHandler.GetLogs(ctx, w3c.Client.Eth, fq)
		metrics.Registry.RPC.StoreHit("eth_getLogs", "store").Mark(hitStore)
		if logs == nil { // uniform empty logs
			logs = ethEmptyLogs
		}

		return logs, false, err
	}

	// fail over to fullnode if no handler configured
	logs, err := w3c.Eth.Logs(*fq)
	return logs, true, err
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
) (*web3Types.TransactionDetail, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.TransactionByBlockHashAndIndex(hash, uint(index))
}

// GetTransactionByBlockNumberAndIndex returns information about a transaction by block number and
// transaction index position.
func (api *ethAPI) GetTransactionByBlockNumberAndIndex(
	ctx context.Context, blockNum web3Types.BlockNumber, index hexutil.Uint,
) (*web3Types.TransactionDetail, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update1(&blockNum, "eth_getTransactionByBlockNumberAndIndex", w3c.Eth)
	return w3c.Eth.TransactionByBlockNumberAndIndex(blockNum, uint(index))
}

// TODO: This method should be removed once `web3Types.FilterQuery` is logging friendly.
func (api *ethAPI) filterLogger(filter *web3Types.FilterQuery) *logrus.Entry {
	logger := logrus.WithField("filter", filter)

	if filter.FromBlock != nil {
		logger = logger.WithField("fromBlock", filter.FromBlock.Int64())
	}

	if filter.ToBlock != nil {
		logger = logger.WithField("toBlock", filter.ToBlock.Int64())
	}

	return logger
}

// The following RPC methods are not supported yet by the fullnode:
// `eth_feeHistory`
// `eth_getFilterChanges`
// `eth_getFilterLogs`
// `eth_newBlockFilter`
// `eth_newFilter`
// `eth_newPendingTransactionFilter`
// `eth_uninstallFilter`
