package rpc

import (
	"context"
	"math/big"
	"math/bits"

	"github.com/conflux-chain/conflux-infura/node"
	"github.com/conflux-chain/conflux-infura/rpc/cache"
	"github.com/conflux-chain/conflux-infura/rpc/handler"
	"github.com/conflux-chain/conflux-infura/store"
	"github.com/conflux-chain/conflux-infura/util"
	"github.com/conflux-chain/conflux-infura/util/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/client"
	web3Types "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	ethEmptyLogs = []web3Types.Log{}

	ethHardforkBlockNumberDevnet    = rpc.BlockNumber(1)
	ethChainId2HardforkBlockNumbers = map[uint64]rpc.BlockNumber{
		1030: 36935000, //mainnet
		71:   61465000, // testnet
	}
)

type EthAPIOption struct {
	StoreHandler  *handler.EthStoreHandler
	LogApiHandler *handler.EthLogsApiHandler
}

func updateEthStoreHitRatio(method string, hit bool) {
	metrics.Registry.RPC.StoreHit(method, "store").Mark(hit)
}

// ethAPI provides ethereum relative API within EVM space according to:
// https://github.com/Pana/conflux-doc/blob/master/docs/evm_space_zh.md
type ethAPI struct {
	EthAPIOption

	provider         *node.EthClientProvider
	inputBlockMetric metrics.InputBlockMetric

	hardforkBlockNumber *rpc.BlockNumber // return default value before eSpace hardfork
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

	hardforkBlockNumber := &ethHardforkBlockNumberDevnet
	if bn, ok := ethChainId2HardforkBlockNumbers[*chainId]; ok {
		hardforkBlockNumber = &bn
	}

	var opt EthAPIOption
	if len(option) > 0 {
		opt = option[0]
	}

	return &ethAPI{
		EthAPIOption:        opt,
		provider:            provider,
		hardforkBlockNumber: hardforkBlockNumber,
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
		updateEthStoreHitRatio("eth_getBlockByHash", err == nil)
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
		updateEthStoreHitRatio("eth_getBlockByNumber", err == nil)
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
func (api *ethAPI) GetTransactionByHash(ctx context.Context, hash common.Hash) (*web3Types.Transaction, error) {
	logger := logrus.WithField("txHash", hash.Hex())

	if !store.EthStoreConfig().IsChainTxnDisabled() && !util.IsInterfaceValNil(api.StoreHandler) {
		tx, err := api.StoreHandler.GetTransactionByHash(ctx, hash)
		updateEthStoreHitRatio("eth_getTransactionByHash", err == nil)
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
		updateEthStoreHitRatio("eth_getTransactionReceipt", err == nil)
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
func (api *ethAPI) GetLogs(ctx context.Context, filter web3Types.FilterQuery) ([]web3Types.Log, error) {
	w3c := GetEthClientFromContext(ctx)
	api.metricLogFilter(w3c.Eth, &filter)

	flag, ok := store.ParseEthLogFilterType(&filter)
	if !ok {
		return ethEmptyLogs, errInvalidEthLogFilter
	}

	if err := api.normalizeLogFilter(w3c.Client, flag, &filter); err != nil {
		return ethEmptyLogs, err
	}

	if err := api.validateLogFilter(flag, &filter); err != nil {
		api.filterLogger(&filter).
			WithError(err).
			Debug("Invalid log filter parameter for eth_getLogs rpc request")

		return ethEmptyLogs, err
	}

	// return empty directly if filter block range before eSpace hardfork
	if filter.ToBlock != nil && *filter.ToBlock <= *api.hardforkBlockNumber {
		return ethEmptyLogs, nil
	}

	if api.LogApiHandler != nil {
		logs, hitStore, err := api.LogApiHandler.GetLogs(ctx, w3c.Client, filter)

		api.filterLogger(&filter).WithField("hitStore", hitStore).
			WithError(err).
			Debug("Delegated `eth_getLogs` to log api handler")

		updateEthStoreHitRatio("eth_getLogs", hitStore)

		if logs == nil { // uniform empty logs
			logs = ethEmptyLogs
		}

		return logs, err
	}

	// fail over to fullnode if no handler configured

	api.filterLogger(&filter).
		Debug("Fail over `eth_getLogs` to fullnode due to no API handler configured")
	return w3c.Eth.Logs(filter)
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
) (*web3Types.Transaction, error) {
	w3c := GetEthClientFromContext(ctx)
	return w3c.Eth.TransactionByBlockHashAndIndex(hash, uint(index))
}

// GetTransactionByBlockNumberAndIndex returns information about a transaction by block number and
// transaction index position.
func (api *ethAPI) GetTransactionByBlockNumberAndIndex(
	ctx context.Context, blockNum web3Types.BlockNumber, index hexutil.Uint,
) (*web3Types.Transaction, error) {
	w3c := GetEthClientFromContext(ctx)
	api.inputBlockMetric.Update1(&blockNum, "eth_getTransactionByBlockNumberAndIndex", w3c.Eth)
	return w3c.Eth.TransactionByBlockNumberAndIndex(blockNum, uint(index))
}

func (api *ethAPI) normalizeLogFilter(w3c *web3go.Client, flag store.LogFilterType, filter *web3Types.FilterQuery) error {
	// set default block range if not set and normalize block number if necessary
	if flag&store.LogFilterTypeBlockRange != 0 {
		defaultBlockNo := rpc.LatestBlockNumber

		// if no from block provided, set latest block number as default
		if filter.FromBlock == nil {
			filter.FromBlock = &defaultBlockNo
		}

		// if no to block provided, set latest block number as default
		if filter.ToBlock == nil {
			filter.ToBlock = &defaultBlockNo
		}

		var blocks [2]*rpc.BlockNumber
		for i, b := range []*rpc.BlockNumber{filter.FromBlock, filter.ToBlock} {
			block, err := util.NormalizeEthBlockNumber(w3c, b, *api.hardforkBlockNumber)
			if err != nil {
				return errors.WithMessage(err, "failed to normalize block number")
			}

			blocks[i] = block
		}

		filter.FromBlock, filter.ToBlock = blocks[0], blocks[1]
	}

	// For store v2, filter offset/limit is not supported anymore.
	// TODO: remove the following reset codes once fullnode v2.0.3 is ready.
	if api.LogApiHandler != nil && api.LogApiHandler.V2() != nil {
		filter.Limit = nil
	}

	return nil
}

func (api *ethAPI) validateLogFilter(flag store.LogFilterType, filter *web3Types.FilterQuery) error {
	// different types of log filters are mutual exclusion
	if bits.OnesCount(uint(flag)) > 1 {
		return errInvalidEthLogFilter
	}

	if flag&store.LogFilterTypeBlockRange != 0 {
		if *filter.FromBlock > *filter.ToBlock {
			return errInvalidLogFilterBlockRange
		}
	}

	if api.LogApiHandler != nil && api.LogApiHandler.V2() != nil {
		// ignore offset/limit && epoch/block range bound for v2
		return nil
	}

	if count := *filter.ToBlock - *filter.FromBlock + 1; uint64(count) > store.MaxLogEpochRange {
		return errExceedLogFilterBlockRangeSize(store.MaxLogEpochRange)
	}

	// TODO: remove the following reset codes once fullnode v2.0.3 is ready.
	if filter.Limit != nil && uint64(*filter.Limit) > store.MaxLogLimit {
		return errors.Errorf(
			"limit set exceeds max acceptable value %v", store.MaxLogLimit,
		)
	}

	return nil
}

func (api *ethAPI) metricLogFilter(eth *client.RpcEthClient, filter *web3Types.FilterQuery) {
	metrics.Registry.RPC.Percentage("eth_getLogs", "filter/hash").Mark(filter.BlockHash != nil)
	metrics.Registry.RPC.Percentage("eth_getLogs", "filter/address/null").Mark(len(filter.Addresses) == 0)
	metrics.Registry.RPC.Percentage("eth_getLogs", "address/single").Mark(len(filter.Addresses) == 1)
	metrics.Registry.RPC.Percentage("eth_getLogs", "address/multiple").Mark(len(filter.Addresses) > 1)
	metrics.Registry.RPC.Percentage("eth_getLogs", "filter/topics").Mark(len(filter.Topics) > 0)

	if filter.BlockHash == nil {
		api.inputBlockMetric.Update1(filter.FromBlock, "eth_getLogs/from", eth)
		api.inputBlockMetric.Update1(filter.ToBlock, "eth_getLogs/to", eth)
	}
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
